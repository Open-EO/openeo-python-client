import concurrent
import logging
from datetime import datetime
from typing import List, Union, Iterable

import pandas as pd
import geopandas as gpd
import pystac
import requests
from pystac import Collection, Item
from pystac_client import Client
from requests.auth import HTTPBasicAuth
from shapely.geometry import shape, mapping

from openeo.extra.job_management import JobDatabaseInterface

_log = logging.getLogger(__name__)

class STACAPIJobDatabase(JobDatabaseInterface):
    """
    Persist/load job metadata from a STAC API

    Unstable API, subject to change.

    :implements: :py:class:`JobDatabaseInterface`
    """

    def __init__(self, collection_id:str, stac_root_url:str, username,password):
        self.collection_id = collection_id
        self.client = Client.open(stac_root_url)
        self._auth = HTTPBasicAuth(username,password)
        self.base_url = stac_root_url
        #self.collection = self.client.get_collection(collection_id)



    def exists(self) -> bool:
        return len([c.id for c in self.client.get_collections() if c.id == self.collection_id ]) >0

    @staticmethod
    def series_from(item):
        """Convert item to a pandas.Series

        Args:
            item (pystac.Item): STAC Item to be converted.

        Returns:
            pandas.Series

        """
        item_dict = item.to_dict()
        item_id = item_dict["id"]
        print(item_dict)
        # Promote datetime
        dt = item_dict["properties"]["datetime"]
        item_dict["datetime"] = pystac.utils.str_to_datetime(dt)
        #del item_dict["properties"]["datetime"]


        # Convert geojson geom into shapely.Geometry

        item_dict["properties"]["geometry"] = shape(item_dict["geometry"])
        item_dict["properties"]["name"] = item_id
        return pd.Series(item_dict["properties"], name=item_id)

    @staticmethod
    def item_from(series, geometry_name="geometry"):

        series_dict = series.to_dict()
        item_dict = {}
        item_dict.setdefault("stac_version", pystac.get_stac_version())
        item_dict.setdefault("type", "Feature")
        item_dict.setdefault("assets", {})
        item_dict.setdefault("links", [])
        item_dict.setdefault("properties", series_dict)

        dt = series_dict.get("datetime", None)
        if dt and item_dict["properties"].get("datetime", None) is None:
            dt_str = pystac.utils.datetime_to_str(dt) if isinstance(dt, datetime) else dt
            item_dict["properties"]["datetime"] = dt_str

        else:
            item_dict["properties"]["datetime"] = pystac.utils.datetime_to_str(datetime.now())

        item_dict["geometry"] = mapping(series[geometry_name])
        del series_dict[geometry_name]

        # from_dict handles associating any Links and Assets with the Item
        item_dict['id'] = series['name']
        del series_dict['name']
        item = pystac.Item.from_dict(item_dict)
        item.bbox = series[geometry_name].bounds
        return item

    def count_by_status(self, statuses: List[str]) -> dict:
        #todo: replace with use of stac aggregation extension
        #example of how what an aggregation call looks like: https://stac-openeo-dev.vgt.vito.be/collections/copernicus_r_utm-wgs84_10_m_hrvpp-vpp_p_2017-now_v01/aggregate?aggregations=total_count&filter=description%3DSOSD&filter-lang=cql2-text
        items = self.get_by_status(statuses,max=200)
        return items.groupby("status").count().to_dict()

    def get_by_status(self, statuses: List[str], max=None) -> pd.DataFrame:

        if isinstance(statuses,str):
            statuses = [statuses]

        status_filter =  " OR ".join([ f"\"properties.status\"={s}" for s in statuses])
        search_results = self.client.search(
            method="GET",
            collections=[self.collection_id],
            filter=status_filter,
            max_items=max,
            fields=["properties"]
        )

        print(search_results.url_with_parameters())
        crs = "EPSG:4326"
        series = [STACAPIJobDatabase.series_from(item) for item in search_results.items()]
        gdf = gpd.GeoDataFrame(series, crs=crs)
        # TODO how to know the proper name of the geometry column?
        # this only matters for the udp based version probably
        gdf.rename_geometry("polygon", inplace=True)
        return gdf



    def persist(self, df: pd.DataFrame):

        if not self.exists():
            c= pystac.Collection(id=self.collection_id,description="test collection for jobs",extent=pystac.Extent(spatial=pystac.SpatialExtent(bboxes=[list(df.total_bounds)]),temporal=pystac.TemporalExtent(intervals=[None,None])))
            self._create_collection(c)

        all_items = []
        def handle_row(series):
            item = STACAPIJobDatabase.item_from(series,df.geometry.name)
            #upload item
            all_items.append(item)


        df.apply(handle_row, axis=1)

        self._upload_items_bulk(self.collection_id, all_items)

    def _prepare_item(self, item: Item, collection_id: str):
        item.collection_id = collection_id

        if not item.get_links(pystac.RelType.COLLECTION):
            item.add_link(pystac.Link(rel=pystac.RelType.COLLECTION, target=item.collection_id))

    def _ingest_bulk(self, items: Iterable[Item]) -> dict:
        collection_id = items[0].collection_id
        if not all(i.collection_id == collection_id for i in items):
            raise Exception("All collection IDs should be identical for bulk ingests")

        url_path = f"collections/{collection_id}/bulk_items"
        data = {"method": "upsert", "items": {item.id: item.to_dict() for item in items}}
        response = requests.post(self.join_url(url_path), auth=self._auth, json=data)

        _log.info(f"HTTP response: {response.status_code} - {response.reason}: body: {response.json()}")

        _check_response_status(response, _EXPECTED_STATUS_POST)
        return response.json()

    def _upload_items_bulk(self, collection_id: str, items: Iterable[Item]) -> None:
        chunk = []
        chunk_start = 0
        chunk_end = 0
        futures = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            for index, item in enumerate(items):
                self._prepare_item(item, collection_id)
                # item.validate()
                chunk.append(item)

                if len(chunk) == self.bulk_size:
                    chunk_end = index + 1
                    chunk_start = chunk_end - len(chunk) + 1

                    futures.append(executor.submit(self._ingest_bulk, chunk.copy()))
                    chunk = []

            if chunk:
                chunk_end = index + 1
                chunk_start = chunk_end - len(chunk) + 1

                self._ingest_bulk(chunk)

            for _ in concurrent.futures.as_completed(futures):
                continue

    def join_url(self, url_path: Union[str, list[str]]) -> str:
        """Create a URL from the base_url and the url_path.

        :param url_path: same as in join_path
        :return: a URL object that represents the full URL.
        """
        return str(self.base_url / "/".join(url_path))

    def _create_collection(self, collection: Collection) -> dict:
        """Create a new collection.

        :param collection: pystac.Collection object to create in the STAC API backend (or upload if you will)
        :raises TypeError: if collection is not a pystac.Collection.
        :return: dict that contains the JSON body of the HTTP response.
        """

        if not isinstance(collection, Collection):
            raise TypeError(
                f'Argument "collection" must be of type pystac.Collection, but its type is {type(collection)=}'
            )

        collection.validate()
        coll_dict = collection.to_dict()

        default_auth = {
            "_auth": {
                "read": ["anonymous"],
                "write": ["stac-openeo-admin", "stac-openeo-editor"]
            }
        }

        coll_dict.update(default_auth)

        response = requests.post(self.join_url("collections"), auth=self._auth,json=coll_dict)
        _check_response_status(response, _EXPECTED_STATUS_POST)

        return response.json()


_EXPECTED_STATUS_GET = [requests.status_codes.codes.ok]
_EXPECTED_STATUS_POST = [
    requests.status_codes.codes.ok,
    requests.status_codes.codes.created,
    requests.status_codes.codes.accepted,
]
_EXPECTED_STATUS_PUT = [
    requests.status_codes.codes.ok,
    requests.status_codes.codes.created,
    requests.status_codes.codes.accepted,
    requests.status_codes.codes.no_content,
]

def _check_response_status(response: requests.Response, expected_status_codes: list[int], raise_exc: bool = False):
    if response.status_code not in expected_status_codes:
        message = (
            f"Expecting HTTP status to be any of {expected_status_codes} "
            + f"but received {response.status_code} - {response.reason}, request method={response.request.method}\n"
            + f"response body:\n{response.text}"
        )
        if raise_exc:
            raise Exception(message)
        else:
            _log.warning(message)

    # Always raise errors on 4xx and 5xx status codes.
    response.raise_for_status()