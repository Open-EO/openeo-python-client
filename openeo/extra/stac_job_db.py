from datetime import datetime
from typing import List

import pandas as pd
import geopandas as gpd
import pystac
from pystac_client import Client
from shapely.geometry import shape, mapping

from openeo.extra.job_management import JobDatabaseInterface

class STACAPIJobDatabase(JobDatabaseInterface):
    """
    Persist/load job metadata from a STAC API

    Unstable API, subject to change.

    :implements: :py:class:`JobDatabaseInterface`
    """

    def __init__(self, collection_id:str, stac_root_url:str):
        self.collection_id = collection_id

        api_url = stac_root_url
        self.client = Client.open(api_url)
        #self.collection = self.client.get_collection(collection_id)
        from stacbuilder import AuthSettings, Settings, Uploader
        auth_settings = AuthSettings(
            enabled=True,
            interactive=False,
            token_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/token",
            authorization_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/auth",
            client_id="terracatalogueclient",
            username="xxx",
            password="yyy",
        )
        settings = Settings(
            auth=auth_settings,
            stac_api_url=stac_root_url,
            collection_auth_info={
                "_auth": {
                    "read": ["anonymous"],
                    "write": ["stac-openeo-admin", "stac-openeo-editor"]
                }
            },
            bulk_size=1000,
        )

        self.uploader = Uploader.from_settings(settings)


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
        items = self.get_by_status(statuses,max=200)
        return items.groupby("status").count().to_dict()

    def get_by_status(self, statuses: List[str], max=None) -> pd.DataFrame:

        status_filter =  " OR ".join([ f"status = {s}" for s in statuses])
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
            self.uploader.upload_collection(c)

        all_items = []
        def handle_row(series):
            item = STACAPIJobDatabase.item_from(series,df.geometry.name)
            #upload item
            all_items.append(item)


        df.apply(handle_row, axis=1)

        self.uploader.upload_items_bulk(self.collection_id, all_items)
