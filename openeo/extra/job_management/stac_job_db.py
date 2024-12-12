import concurrent.futures
import datetime
import logging
from typing import Iterable, List

import geopandas as gpd
import numpy as np
import pandas as pd
import pystac
import pystac_client
import requests
from shapely.geometry import mapping, shape

from openeo.extra.job_management import JobDatabaseInterface, MultiBackendJobManager

_log = logging.getLogger(__name__)


class STACAPIJobDatabase(JobDatabaseInterface):
    """
    Persist/load job metadata from a STAC API

    Unstable API, subject to change.

    :implements: :py:class:`JobDatabaseInterface`
    """

    def __init__(
        self,
        collection_id: str,
        stac_root_url: str,
        auth: requests.auth.AuthBase,
        has_geometry: bool = False,
        geometry_column: str = "geometry",
    ):
        """
        Initialize the STACAPIJobDatabase.

        :param collection_id: The ID of the STAC collection.
        :param stac_root_url: The root URL of the STAC API.
        :param auth: requests AuthBase that will be used to authenticate, e.g. OAuth2ResourceOwnerPasswordCredentials
        :param has_geometry: Whether the job metadata supports any geometry that implements __geo_interface__.
        :param geometry_column: The name of the geometry column in the job metadata that implements __geo_interface__.
        """
        self.collection_id = collection_id
        self.client = pystac_client.Client.open(stac_root_url)

        self._auth = auth
        self.has_geometry = has_geometry
        self.geometry_column = geometry_column
        self.base_url = stac_root_url
        self.bulk_size = 500

    def exists(self) -> bool:
        return any(c.id == self.collection_id for c in self.client.get_collections())

    def initialize_from_df(self, df: pd.DataFrame, *, on_exists: str = "error"):
        """
        Initialize the job database from a given dataframe,
        which will be first normalized to be compatible
        with :py:class:`MultiBackendJobManager` usage.

        :param df: dataframe with some columns your ``start_job`` callable expects
        :param on_exists: what to do when the job database already exists (persisted on disk):
            - "error": (default) raise an exception
            - "skip": work with existing database, ignore given dataframe and skip any initialization
            - "append": add given dataframe to existing database

        :return: initialized job database.
        """
        if isinstance(df, gpd.GeoDataFrame):
            df = df.copy()
            _log.warning("Job Database is initialized from GeoDataFrame. Converting geometries to GeoJSON.")
            self.geometry_column = df.geometry.name
            df[self.geometry_column] = df[self.geometry_column].apply(lambda x: mapping(x))
            df = pd.DataFrame(df)
            self.has_geometry = True

        if self.exists():
            if on_exists == "skip":
                return self
            elif on_exists == "error":
                raise FileExistsError(f"Job database {self!r} already exists.")
            elif on_exists == "append":
                existing_df = self.get_by_status([])
                df = MultiBackendJobManager._normalize_df(df)
                df = pd.concat([existing_df, df], ignore_index=True).replace({np.nan: None})
                self.persist(df)
                return self

            else:
                raise ValueError(f"Invalid on_exists={on_exists!r}")

        df = MultiBackendJobManager._normalize_df(df)
        self.persist(df)
        # Return self to allow chaining with constructor.
        return self

    def series_from(self, item: pystac.Item) -> pd.Series:
        """
        Convert a STAC Item to a pandas.Series.

        :param item: STAC Item to be converted.
        :return: pandas.Series
        """
        item_dict = item.to_dict()
        item_id = item_dict["id"]
        dt = item_dict["properties"]["datetime"]

        return pd.Series(item_dict["properties"], name=item_id)

    def item_from(self, series: pd.Series) -> pystac.Item:
        """
        Convert a pandas.Series to a STAC Item.

        :param series: pandas.Series to be converted.
        :param geometry_name: Name of the geometry column in the series.
        :return: pystac.Item
        """
        series_dict = series.to_dict()
        item_dict = {}
        item_dict.setdefault("stac_version", pystac.get_stac_version())
        item_dict.setdefault("type", "Feature")
        item_dict.setdefault("assets", {})
        item_dict.setdefault("links", [])
        item_dict.setdefault("properties", series_dict)

        dt = series_dict.get("datetime", None)
        if dt and item_dict["properties"].get("datetime", None) is None:
            dt_str = pystac.utils.datetime_to_str(dt) if isinstance(dt, datetime.datetime) else dt
            item_dict["properties"]["datetime"] = dt_str

        else:
            item_dict["properties"]["datetime"] = pystac.utils.datetime_to_str(datetime.datetime.now())

        if self.has_geometry:
            item_dict["geometry"] = series[self.geometry_column]
        else:
            item_dict["geometry"] = None

        # from_dict handles associating any Links and Assets with the Item
        item_dict["id"] = series.name
        item = pystac.Item.from_dict(item_dict)
        if self.has_geometry:
            item.bbox = shape(series[self.geometry_column]).bounds
        else:
            item.bbox = None
        return item

    def count_by_status(self, statuses: Iterable[str] = ()) -> dict:
        if isinstance(statuses, str):
            statuses = {statuses}
        statuses = set(statuses)
        items = self.get_by_status(statuses, max=200)
        if items is None:
            return {k: 0 for k in statuses}
        else:
            return items["status"].value_counts().to_dict()

    def get_by_status(self, statuses: Iterable[str], max=None) -> pd.DataFrame:
        if isinstance(statuses, str):
            statuses = {statuses}
        statuses = set(statuses)

        status_filter = " OR ".join([f"\"properties.status\"='{s}'" for s in statuses]) if statuses else None
        search_results = self.client.search(
            method="GET",
            collections=[self.collection_id],
            filter=status_filter,
            max_items=max,
        )

        series = [self.series_from(item) for item in search_results.items()]

        df = pd.DataFrame(series)
        if len(series) == 0:
            # TODO: What if default columns are overwritten by the user?
            df = MultiBackendJobManager._normalize_df(
                df
            )  # Even for an empty dataframe the default columns are required
        return df

    def persist(self, df: pd.DataFrame):
        if not self.exists():
            spatial_extent = pystac.SpatialExtent([[-180, -90, 180, 90]])
            temporal_extent = pystac.TemporalExtent([[None, None]])
            extent = pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)
            c = pystac.Collection(id=self.collection_id, description="STAC API job database collection.", extent=extent)
            self._create_collection(c)

        all_items = []
        if not df.empty:

            def handle_row(series):
                item = self.item_from(series)
                all_items.append(item)

            df.apply(handle_row, axis=1)

        self._upload_items_bulk(self.collection_id, all_items)

    def _prepare_item(self, item: pystac.Item, collection_id: str):
        item.collection_id = collection_id

        if not item.get_links(pystac.RelType.COLLECTION):
            item.add_link(pystac.Link(rel=pystac.RelType.COLLECTION, target=item.collection_id))

    def _ingest_bulk(self, items: List[pystac.Item]) -> dict:
        collection_id = items[0].collection_id
        if not all(i.collection_id == collection_id for i in items):
            raise Exception("All collection IDs should be identical for bulk ingests")

        url_path = f"collections/{collection_id}/bulk_items"
        data = {"method": "upsert", "items": {item.id: item.to_dict() for item in items}}
        response = requests.post(url=self.join_url(url_path), auth=self._auth, json=data)

        _log.info(f"HTTP response: {response.status_code} - {response.reason}: body: {response.json()}")

        _check_response_status(response, _EXPECTED_STATUS_POST)
        return response.json()

    def _upload_items_bulk(self, collection_id: str, items: List[pystac.Item]) -> None:
        chunk = []
        futures = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            for item in items:
                self._prepare_item(item, collection_id)
                chunk.append(item)

                if len(chunk) == self.bulk_size:
                    futures.append(executor.submit(self._ingest_bulk, chunk.copy()))
                    chunk = []

            if chunk:
                self._ingest_bulk(chunk)

            for _ in concurrent.futures.as_completed(futures):
                continue

    def join_url(self, url_path: str) -> str:
        """Create a URL from the base_url and the url_path.

        :param url_path: same as in join_path
        :return: a URL object that represents the full URL.
        """
        return str(self.base_url + "/" + url_path)

    def _create_collection(self, collection: pystac.Collection) -> dict:
        """Create a new collection.

        :param collection: pystac.Collection object to create in the STAC API backend (or upload if you will)
        :raises TypeError: if collection is not a pystac.Collection.
        :return: dict that contains the JSON body of the HTTP response.
        """

        if not isinstance(collection, pystac.Collection):
            raise TypeError(
                f'Argument "collection" must be of type pystac.Collection, but its type is {type(collection)=}'
            )

        collection.validate()
        coll_dict = collection.to_dict()

        default_auth = {
            "_auth": {
                "read": ["anonymous"],
                "write": ["stac-openeo-admin", "stac-openeo-editor"],
            }
        }

        coll_dict.update(default_auth)

        response = requests.post(self.join_url("collections"), auth=self._auth, json=coll_dict)
        _check_response_status(response, _EXPECTED_STATUS_POST)

        return response.json()


_EXPECTED_STATUS_POST = [
    requests.status_codes.codes.ok,
    requests.status_codes.codes.created,
    requests.status_codes.codes.accepted,
]


def _check_response_status(response: requests.Response, expected_status_codes: List[int], raise_exc: bool = False):
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
