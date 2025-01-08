import datetime
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pandas.testing as pdt
import pystac
import pystac_client
import pytest
from requests.auth import AuthBase
from shapely.geometry import Point

from openeo.extra.job_management import MultiBackendJobManager
from openeo.extra.job_management.stac_job_db import STACAPIJobDatabase


@pytest.fixture
def mock_auth():
    return MagicMock(spec=AuthBase)


@pytest.fixture
def mock_stac_api_job_database(mock_auth) -> STACAPIJobDatabase:
    return STACAPIJobDatabase(collection_id="test_id", stac_root_url="http://fake-stac-api.test", auth=mock_auth)


@pytest.fixture
def mock_pystac_client(dummy_stac_item):
    mock_client = MagicMock(spec=pystac_client.Client)

    mock_client.get_collections.return_value = [
        MagicMock(id="collection-1"),
        MagicMock(id="collection-2"),
    ]

    mock_item_search = MagicMock(spec=pystac_client.ItemSearch)
    mock_item_search.items.return_value = [dummy_stac_item]
    mock_client.search.return_value = mock_item_search

    with patch("pystac_client.Client.open", return_value=mock_client):
        yield mock_client


@pytest.fixture
def job_db_exists(mock_pystac_client) -> STACAPIJobDatabase:
    return STACAPIJobDatabase(
        collection_id="collection-1",
        stac_root_url="http://fake-stac-api",
        auth=None,
    )


@pytest.fixture
def job_db_not_exists(mock_pystac_client) -> STACAPIJobDatabase:
    return STACAPIJobDatabase(
        collection_id="collection-3",
        stac_root_url="http://fake-stac-api",
        auth=None,
        has_geometry=False,
    )


@pytest.fixture
def dummy_dataframe() -> pd.DataFrame:
    return pd.DataFrame({"no": [1], "geometry": [2], "here": [3]})


@pytest.fixture
def normalized_dummy_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "no": [1],
            "geometry": [2],
            "here": [3],
            "id": None,
            "backend_name": None,
            "status": ["not_started"],
            "start_time": None,
            "running_start_time": None,
            "cpu": None,
            "memory": None,
            "duration": None,
            "costs": None,
        },
    )


@pytest.fixture
def another_dummy_dataframe() -> pd.DataFrame:
    return pd.DataFrame({"no": [4], "geometry": [5], "here": [6]})


@pytest.fixture
def normalized_merged_dummy_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "no": [1, 4],
            "geometry": [2, 5],
            "here": [3, 6],
            "id": None,
            "backend_name": None,
            "status": ["not_started", "not_started"],
            "start_time": None,
            "running_start_time": None,
            "cpu": None,
            "memory": None,
            "duration": None,
            "costs": None,
        }
    )


@pytest.fixture
def dummy_geodataframe() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {
            "there": [1],
            "is": [2],
            "geometry": [Point(1, 1)],
        },
        geometry="geometry",
    )


@pytest.fixture
def normalized_dummy_geodataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "there": [1],
            "is": [2],
            "geometry": [{"type": "Point", "coordinates": (1.0, 1.0)}],
            "id": None,
            "backend_name": None,
            "status": ["not_started"],
            "start_time": None,
            "running_start_time": None,
            "cpu": None,
            "memory": None,
            "duration": None,
            "costs": None,
        }
    )


FAKE_NOW = datetime.datetime(2020, 5, 22)


@pytest.fixture
def dummy_stac_item() -> pystac.Item:
    properties = {
        "datetime": pystac.utils.datetime_to_str(FAKE_NOW),
        "some_property": "value",
    }

    return pystac.Item(id="test", geometry=None, bbox=None, properties=properties, datetime=FAKE_NOW)


@pytest.fixture
def dummy_stac_item_geometry() -> pystac.Item:
    properties = {
        "datetime": pystac.utils.datetime_to_str(FAKE_NOW),
        "some_property": "value",
        "geometry": {"type": "Polygon", "coordinates": (((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)),)},
    }

    return pystac.Item(
        id="test",
        geometry={"type": "Polygon", "coordinates": (((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)),)},
        bbox=(0.0, 0.0, 1.0, 1.0),
        properties=properties,
        datetime=FAKE_NOW,
    )


@pytest.fixture
def dummy_series() -> pd.Series:
    return pd.Series({"datetime": pystac.utils.datetime_to_str(FAKE_NOW), "some_property": "value"}, name="test")


@pytest.fixture
def dummy_series_geometry() -> pd.Series:
    return pd.Series(
        {
            "datetime": pystac.utils.datetime_to_str(FAKE_NOW),
            "some_property": "value",
            "geometry": {
                "type": "Polygon",
                "coordinates": (((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)),),
            },
        },
        name="test",
    )


@pytest.fixture
def patch_datetime_now():
    with patch("datetime.datetime") as mock_datetime:
        mock_datetime.now.return_value = FAKE_NOW
        yield mock_datetime


@pytest.fixture
def bulk_dataframe():
    return pd.DataFrame(
        {
            "some_property": [f"value-{i}" for i in range(10)],
        },
        index=[i for i in range(10)],
    )


class TestSTACAPIJobDatabase:
    def test_exists(self, job_db_exists, job_db_not_exists):

        assert job_db_exists.exists() == True
        assert job_db_not_exists.exists() == False

    @patch("openeo.extra.job_management.stac_job_db.STACAPIJobDatabase.persist", return_value=None)
    def test_initialize_from_df_non_existing(
        self, mock_persist, job_db_not_exists, dummy_dataframe, normalized_dummy_dataframe
    ):

        job_db_not_exists.initialize_from_df(dummy_dataframe)

        mock_persist.assert_called_once()
        pdt.assert_frame_equal(mock_persist.call_args[0][0], normalized_dummy_dataframe)
        assert job_db_not_exists.has_geometry == False

    def test_initialize_from_df_existing_error(self, job_db_exists, dummy_dataframe):
        with pytest.raises(FileExistsError):
            job_db_exists.initialize_from_df(dummy_dataframe)

    @patch("openeo.extra.job_management.stac_job_db.STACAPIJobDatabase.persist", return_value=None)
    @patch("openeo.extra.job_management.stac_job_db.STACAPIJobDatabase.get_by_status")
    def test_initialize_from_df_existing_append(
        self,
        mock_get_by_status,
        mock_persist,
        job_db_exists,
        normalized_dummy_dataframe,
        another_dummy_dataframe,
        normalized_merged_dummy_dataframe,
    ):
        mock_get_by_status.return_value = normalized_dummy_dataframe
        job_db_exists.initialize_from_df(another_dummy_dataframe, on_exists="append")

        mock_persist.assert_called_once()
        pdt.assert_frame_equal(mock_persist.call_args[0][0], normalized_merged_dummy_dataframe)
        assert job_db_exists.has_geometry == False

    @patch("openeo.extra.job_management.stac_job_db.STACAPIJobDatabase.persist", return_value=None)
    def test_initialize_from_df_with_geometry(
        self, mock_persists, job_db_not_exists, dummy_geodataframe, normalized_dummy_geodataframe
    ):
        job_db_not_exists.initialize_from_df(dummy_geodataframe)

        mock_persists.assert_called_once()
        pdt.assert_frame_equal(mock_persists.call_args[0][0], normalized_dummy_geodataframe)
        assert job_db_not_exists.has_geometry == True
        assert job_db_not_exists.geometry_column == "geometry"

    def test_series_from(self, job_db_exists, dummy_series, dummy_stac_item):
        pdt.assert_series_equal(job_db_exists.series_from(dummy_stac_item), dummy_series)

    def test_item_from(self, patch_datetime_now, job_db_exists, dummy_series, dummy_stac_item):
        item = job_db_exists.item_from(dummy_series)
        assert item.to_dict() == dummy_stac_item.to_dict()

    def test_item_from_geometry(
        self, patch_datetime_now, job_db_exists, dummy_series_geometry, dummy_stac_item_geometry
    ):
        job_db_exists.has_geometry = True
        item = job_db_exists.item_from(dummy_series_geometry)
        assert item.to_dict() == dummy_stac_item_geometry.to_dict()

    @patch("openeo.extra.job_management.stac_job_db.STACAPIJobDatabase.get_by_status")
    def test_count_by_status(self, mock_get_by_status, normalized_dummy_dataframe, job_db_exists):
        mock_get_by_status.return_value = normalized_dummy_dataframe
        assert job_db_exists.count_by_status() == {"not_started": 1}

    def test_get_by_status_no_filter(self, job_db_exists):
        job_db_exists.get_by_status(())

        job_db_exists.client.search.assert_called_once_with(
            method="GET", collections=["collection-1"], filter=None, max_items=None
        )

    def test_get_by_status_with_filter(self, job_db_exists):
        job_db_exists.get_by_status(["not_started"])
        job_db_exists.client.search.assert_called_once_with(
            method="GET", collections=["collection-1"], filter="\"properties.status\"='not_started'", max_items=None
        )

    def test_get_by_status_result(self, job_db_exists):
        df = job_db_exists.get_by_status(["not_started"])

        pdt.assert_frame_equal(
            df,
            pd.DataFrame(
                {
                    "datetime": [pystac.utils.datetime_to_str(FAKE_NOW)],
                    "some_property": ["value"],
                },
                index=["test"],
            ),
        )

    @patch("requests.post")
    def test_persist_single_chunk(self, mock_requests_post, bulk_dataframe, job_db_exists, patch_datetime_now):
        def bulk_items(df):
            all_items = []
            if not df.empty:

                def handle_row(series):
                    item = job_db_exists.item_from(series)
                    job_db_exists._prepare_item(item, job_db_exists.collection_id)
                    all_items.append(item)

                df.apply(handle_row, axis=1)
            return all_items

        items = bulk_items(bulk_dataframe)

        mock_requests_post.return_value.status_code = 200
        mock_requests_post.return_value.json.return_value = {"status": "success"}
        mock_requests_post.reason = "OK"

        job_db_exists.persist(bulk_dataframe)
        # job_db_exists._upload_items_bulk(collection_id=job_db_exists.collection_id, items=items)

        mock_requests_post.assert_called_once()

        mock_requests_post.assert_called_with(
            url=f"http://fake-stac-api/collections/{job_db_exists.collection_id}/bulk_items",
            auth=None,
            json={
                "method": "upsert",
                "items": {item.id: item.to_dict() for item in items},
            },
        )

    @patch("requests.post")
    def test_persist_multiple_chunks(self, mock_requests_post, bulk_dataframe, job_db_exists):
        def bulk_items(df):
            all_items = []
            if not df.empty:

                def handle_row(series):
                    item = job_db_exists.item_from(series)
                    job_db_exists._prepare_item(item, job_db_exists.collection_id)
                    all_items.append(item)

                df.apply(handle_row, axis=1)
            return all_items

        items = bulk_items(bulk_dataframe)

        mock_requests_post.return_value.status_code = 200
        mock_requests_post.return_value.json.return_value = {"status": "success"}
        mock_requests_post.reason = "OK"

        job_db_exists.bulk_size = 3
        job_db_exists._upload_items_bulk(collection_id=job_db_exists.collection_id, items=items)

        # 10 items in total, 3 items per chunk, should result in 4 calls
        assert mock_requests_post.call_count == 4
        expected_calls = [
            {
                "url": f"http://fake-stac-api/collections/{job_db_exists.collection_id}/bulk_items",
                "auth": None,
                "json": {"method": "upsert", "items": {item.id: item.to_dict() for item in items[:3]}},
            },
            {
                "url": f"http://fake-stac-api/collections/{job_db_exists.collection_id}/bulk_items",
                "auth": None,
                "json": {"method": "upsert", "items": {item.id: item.to_dict() for item in items[3:6]}},
            },
            {
                "url": f"http://fake-stac-api/collections/{job_db_exists.collection_id}/bulk_items",
                "auth": None,
                "json": {"method": "upsert", "items": {item.id: item.to_dict() for item in items[6:9]}},
            },
            {
                "url": f"http://fake-stac-api/collections/{job_db_exists.collection_id}/bulk_items",
                "auth": None,
                "json": {"method": "upsert", "items": {item.id: item.to_dict() for item in items[9:]}},
            },
        ]

        for i, call in enumerate(mock_requests_post.call_args_list):
            assert call[1] == expected_calls[i]
