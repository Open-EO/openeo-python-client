import collections
import datetime
import re
from typing import Any, Dict, List, Optional, Union
from unittest import mock
from unittest.mock import MagicMock, patch

import dirty_equals
import geopandas as gpd
import pandas as pd
import pandas.testing as pdt
import pystac
import pystac_client
import pytest
from shapely.geometry import Point

from openeo.extra.job_management import MultiBackendJobManager
from openeo.extra.job_management.stac_job_db import STACAPIJobDatabase
from openeo.rest._testing import DummyBackend


@pytest.fixture
def mock_pystac_client():
    mock_client = MagicMock(spec=pystac_client.Client)

    mock_client.get_collections.return_value = [
        MagicMock(id="collection-1"),
        MagicMock(id="collection-2"),
    ]

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


def _common_normalized_df_data(rows: int = 1) -> dict:
    """
    Helper to build a dict (to be passed to `pd.DataFrame`)
    with common columns that are the result of normalization.
    In the context of these tests however, they are
    mainly irrelevant boilerplate data that is tedious to repeat.
    """
    return {
        "id": None,
        "backend_name": None,
        "status": ["not_started"] * rows,
        "start_time": None,
        "running_start_time": None,
        "cpu": None,
        "memory": None,
        "duration": None,
        "costs": None,
    }


def _pystac_item(
    *,
    id: str,
    properties: Optional[dict] = None,
    geometry: Optional = None,
    bbox: Optional = None,
    datetime_: Union[None, str, datetime.date, datetime.date] = "2025-06-07",
) -> pystac.Item:
    """Helper to easily construct a dummy but valid pystac.Item"""
    if isinstance(datetime_, str):
        datetime_ = pystac.utils.str_to_datetime(datetime_)
    elif isinstance(datetime_, datetime.date):
        datetime_ = datetime.datetime.combine(datetime_, datetime.time.min, tzinfo=datetime.timezone.utc)

    return pystac.Item(
        id=id,
        geometry=geometry,
        bbox=bbox,
        properties=properties or {},
        datetime=datetime_,
    )


@pytest.fixture
def bulk_dataframe():
    return pd.DataFrame(
        {
            "item_id": [f"test-{i}" for i in range(10)],
            "some_property": [f"value-{i}" for i in range(10)],
            "datetime": [f"2020-{i+1:02d}-01" for i in range(10)],
        },
        index=[i for i in range(10)],
    )


class TestSTACAPIJobDatabase:
    def test_exists(self, job_db_exists, job_db_not_exists):

        assert job_db_exists.exists() == True
        assert job_db_not_exists.exists() == False

    @patch("openeo.extra.job_management.stac_job_db.STACAPIJobDatabase.persist", return_value=None)
    def test_initialize_from_df_non_existing(self, mock_persist, job_db_not_exists):
        df = pd.DataFrame(
            {
                "no": [1],
                "geometry": [2],
                "here": [3],
            }
        )
        job_db_not_exists.initialize_from_df(df)

        mock_persist.assert_called_once()
        expected = pd.DataFrame(
            {
                "item_id": [0],
                "no": [1],
                "geometry": [2],
                "here": [3],
                **_common_normalized_df_data(),
            },
        )
        pdt.assert_frame_equal(mock_persist.call_args[0][0], expected)
        assert job_db_not_exists.has_geometry == False

    def test_initialize_from_df_existing_error(self, job_db_exists):
        df = pd.DataFrame({"hello": ["world"]})
        with pytest.raises(FileExistsError):
            job_db_exists.initialize_from_df(df)

    @patch("openeo.extra.job_management.stac_job_db.STACAPIJobDatabase.persist", return_value=None)
    @patch("openeo.extra.job_management.stac_job_db.STACAPIJobDatabase.get_by_status")
    def test_initialize_from_df_existing_append(
        self,
        mock_get_by_status,
        mock_persist,
        job_db_exists,
    ):
        mock_get_by_status.return_value = pd.DataFrame(
            {
                "item_id": [0],
                "no": [1],
                "geometry": [2],
                "here": [3],
                **_common_normalized_df_data(),
            },
        )

        df = pd.DataFrame(
            {
                "item_id": [1],
                "no": [4],
                "geometry": [5],
                "here": [6],
            }
        )
        job_db_exists.initialize_from_df(df, on_exists="append")

        mock_persist.assert_called_once()
        expected_df = pd.DataFrame(
            {
                "item_id": [0, 1],
                "no": [1, 4],
                "geometry": [2, 5],
                "here": [3, 6],
                **_common_normalized_df_data(rows=2),
            }
        )
        pdt.assert_frame_equal(mock_persist.call_args[0][0], expected_df)
        assert job_db_exists.has_geometry == False

    @patch("openeo.extra.job_management.stac_job_db.STACAPIJobDatabase.persist", return_value=None)
    def test_initialize_from_df_with_geometry(self, mock_persists, job_db_not_exists):
        df = gpd.GeoDataFrame(
            {
                "there": [1],
                "is": [2],
                "geometry": [Point(1, 1)],
            },
            geometry="geometry",
        )
        job_db_not_exists.initialize_from_df(df)

        mock_persists.assert_called_once()
        expected = pd.DataFrame(
            {
                "item_id": [0],
                "there": [1],
                "is": [2],
                "geometry": [{"type": "Point", "coordinates": (1.0, 1.0)}],
                **_common_normalized_df_data(),
            }
        )
        pdt.assert_frame_equal(mock_persists.call_args[0][0], expected)
        assert job_db_not_exists.has_geometry == True
        assert job_db_not_exists.geometry_column == "geometry"

    def test_series_from(self, job_db_exists):
        item = pystac.Item(
            id="test",
            geometry=None,
            bbox=None,
            properties={
                "datetime": "2020-05-22T00:00:00Z",
                "some_property": "value",
            },
            datetime=datetime.datetime(2020, 5, 22),
        )
        expected = pd.Series({"datetime": "2020-05-22T00:00:00Z", "some_property": "value"}, name="test")
        pdt.assert_series_equal(job_db_exists.series_from(item), expected)

    @pytest.mark.parametrize(
        ["series", "expected"],
        [
            # Minimal (using current time as datetime)
            (
                pd.Series({"item_id": "item-123"}),
                _pystac_item(id="item-123", datetime_="2022-02-02"),
            ),
            # With explicit datetime, and some other properties
            (
                pd.Series(
                    {
                        "item_id": "item-123",
                        "datetime": "2023-04-05",
                        "hello": "world",
                    }
                ),
                _pystac_item(
                    id="item-123",
                    properties={"hello": "world"},
                    datetime_="2023-04-05",
                ),
            ),
            (
                pd.Series(
                    {
                        "item_id": "item-123",
                        "datetime": datetime.datetime(2023, 4, 5, 12, 34),
                        "hello": "world",
                    }
                ),
                _pystac_item(
                    id="item-123",
                    properties={"hello": "world"},
                    datetime_="2023-04-05T12:34:00Z",
                ),
            ),
        ],
    )
    def test_item_from(self, job_db_exists, series, expected, time_machine):
        time_machine.move_to("2022-02-02T00:00:00Z")
        item = job_db_exists.item_from(series)
        assert isinstance(item, pystac.Item)
        assert item.to_dict() == expected.to_dict()

    @pytest.mark.parametrize(
        ["series", "expected"],
        [
            (
                pd.Series(
                    {
                        "item_id": "item-123",
                        "datetime": "2023-04-05",
                        "geometry": {"type": "Polygon", "coordinates": (((1, 2), (3, 4), (0, 5), (1, 2)),)},
                    },
                ),
                _pystac_item(
                    id="item-123",
                    datetime_="2023-04-05",
                    geometry={"type": "Polygon", "coordinates": (((1, 2), (3, 4), (0, 5), (1, 2)),)},
                    bbox=(0.0, 2.0, 3.0, 5.0),
                    properties={
                        "geometry": {"type": "Polygon", "coordinates": (((1, 2), (3, 4), (0, 5), (1, 2)),)},
                    },
                ),
            ),
        ],
    )
    def test_item_from_geometry(self, job_db_exists, series, expected):
        # TODO avoid this `has_geometry` hack?
        job_db_exists.has_geometry = True
        item = job_db_exists.item_from(series)
        assert isinstance(item, pystac.Item)
        assert item.to_dict() == expected.to_dict()

    @patch("openeo.extra.job_management.stac_job_db.STACAPIJobDatabase.get_by_status")
    def test_count_by_status(self, mock_get_by_status, job_db_exists):
        mock_get_by_status.return_value = pd.DataFrame(
            {
                "item_id": [0],
                "status": ["not_started"],
            },
        )
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

    def test_get_by_status_result(self, requests_mock):
        stac_api_url = "http://stacapi.test"
        dummy_stac_api = DummyStacApi(root_url=stac_api_url, requests_mock=requests_mock)
        dummy_stac_api.predefine_collection("collection-123")
        dummy_stac_api.predefine_item(
            collection_id="collection-123",
            item=_pystac_item(id="item-123", properties={"status": "not_started"}),
        )
        dummy_stac_api.predefine_item(
            collection_id="collection-123",
            item=_pystac_item(id="item-456", properties={"status": "running"}),
        )
        dummy_stac_api.predefine_item(
            collection_id="collection-123",
            item=_pystac_item(id="item-789", properties={"status": "not_started"}),
        )

        job_db = STACAPIJobDatabase(collection_id="collection-123", stac_root_url=stac_api_url)

        pdt.assert_frame_equal(
            job_db.get_by_status(["not_started"]),
            pd.DataFrame(
                {
                    "item_id": ["item-123", "item-789"],
                    "status": ["not_started", "not_started"],
                    "datetime": ["2025-06-07T00:00:00Z", "2025-06-07T00:00:00Z"],
                },
            ),
        )
        pdt.assert_frame_equal(
            job_db.get_by_status(["running"]),
            pd.DataFrame(
                {
                    "item_id": ["item-456"],
                    "status": ["running"],
                    "datetime": ["2025-06-07T00:00:00Z"],
                },
            ),
        )

    @patch("requests.post")
    def test_persist_single_chunk(self, mock_requests_post, bulk_dataframe, job_db_exists):
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
        assert sorted(
            (c.kwargs for c in mock_requests_post.call_args_list),
            key=lambda d: sorted(d["json"]["items"].keys()),
        ) == [
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


@pytest.fixture
def dummy_backend_foo(requests_mock) -> DummyBackend:
    dummy = DummyBackend.at_url("https://foo.test", requests_mock=requests_mock)
    dummy.setup_simple_job_status_flow(queued=1, running=2)
    return dummy


@pytest.fixture
def sleep_mock():
    with mock.patch("time.sleep") as sleep:
        yield sleep


class DummyStacApi:
    """Minimal dummy implementation of a STAC API for testing purposes."""

    def __init__(self, *, root_url: str = "http://stacapi.test", requests_mock):
        self.root_url = root_url.rstrip("/")
        self._requests_mock = requests_mock

        requests_mock.get(f"{self.root_url}/", json=self._get_root())
        self.collections: List[dict] = []
        requests_mock.get(f"{self.root_url}/collections", json=self._get_collections)
        requests_mock.post(f"{self.root_url}/collections", json=self._post_collections)

        self.items: Dict[str, Dict[str, Any]] = collections.defaultdict(dict)
        requests_mock.post(
            re.compile(rf"{self.root_url}/collections/[^/]+/bulk_items"), json=self._post_collections_bulk_items
        )

        requests_mock.get(f"{self.root_url}/search?", json=self._get_search)

    def _get_root(self) -> dict:
        """Handler of `GET /` requests."""
        return {
            "stac_version": "1.0.0",
            "id": "dummy-stac-api",
            "title": "Dummy",
            "description": "Dummy STAC API",
            "type": "Catalog",
            "conformsTo": [
                "https://api.stacspec.org/v1.0.0/core",
                "https://api.stacspec.org/v1.0.0/collections",
                "https://api.stacspec.org/v1.0.0/item-search",
            ],
            "links": [],
        }

    def _get_collections(self, request, context):
        """Handler of `GET /collections` requests."""
        return {"collections": self.collections}

    def _post_collections(self, request, context):
        """Handler of `POST /collections` requests."""
        post_data = request.json()
        self.collections.append(post_data)
        return {}

    def predefine_collection(self, collection_id: str):
        """Pre-define a collection with the given ID."""
        assert collection_id not in [c["id"] for c in self.collections]
        self.collections.append(
            pystac.Collection(
                id=collection_id,
                description=collection_id,
                extent=pystac.Extent(
                    spatial=pystac.SpatialExtent([-180, -90, 180, 90]),
                    temporal=pystac.TemporalExtent([[None, None]]),
                ),
                title=collection_id,
            ).to_dict()
        )

    def predefine_item(self, *, collection_id: str, item: pystac.Item):
        if collection_id not in [c["id"] for c in self.collections]:
            self.predefine_collection(collection_id)
        self.items[collection_id][item.id] = item.to_dict()

    def _post_collections_bulk_items(self, request, context):
        """Handler of `POST /collections/{collection_id}/bulk_items` requests."""
        # extract the collection_id from the URL
        collection_id = re.search("/collections/([^/]+)/bulk_items", request.url).group(1)
        post_data = request.json()
        # TODO handle insert/upsert method?
        for item_id, item in post_data["items"].items():
            self.items[collection_id][item_id] = item
        return {}

    def _get_search(self, request, context):
        """Handler of `GET /search` requests."""
        collections = request.qs["collections"][0].split(",")
        items = [
            item
            for cid in collections
            for item in self.items.get(cid, {}).values()
        ]
        if "ids" in request.qs:
            [ids] = request.qs["ids"]
            ids = set(ids.split(","))
            items = [i for i in items if i.get("id") in ids]
        if "filter" in request.qs:
            [property_filter] = request.qs["filter"]
            # TODO: use a more robust CQL2-text parser?
            assert request.qs["filter-lang"] == ["cql2-text"]
            assert re.match(
                r"^\s*\"properties\.status\"='\w+'(\s+or\s+\"properties\.status\"='\w+')*\s*$", property_filter
            )
            statuses = set(re.findall(r"\"properties\.status\"='(\w+)'", property_filter))
            items = [i for i in items if i.get("properties", {}).get("status") in statuses]

        return {
            "type": "FeatureCollection",
            "features": items,
            "links": [],
        }


def test_run_jobs_basic(tmp_path, dummy_backend_foo, requests_mock, sleep_mock):
    job_manager = MultiBackendJobManager(root_dir=tmp_path, poll_sleep=2)
    job_manager.add_backend("foo", connection=dummy_backend_foo.connection)

    stac_api_url = "http://stacapi.test"
    dummy_stac_api = DummyStacApi(root_url=stac_api_url, requests_mock=requests_mock)

    job_db = STACAPIJobDatabase(collection_id="collection-123", stac_root_url=stac_api_url)
    df = pd.DataFrame(
        {
            "item_id": ["item-2024", "item-2025"],
            "year": [2024, 2025],
        }
    )
    job_db.initialize_from_df(df=df)

    def create_job(row, connection, **kwargs):
        year = int(row["year"])
        pg = {"dummy1": {"process_id": "dummy", "arguments": {"year": year}, "result": True}}
        job = connection.create_job(pg)
        return job

    run_stats = job_manager.run_jobs(job_db=job_db, start_job=create_job)

    assert run_stats == dirty_equals.IsPartialDict(
        {
            "job finished": 2,
            "job launch": 2,
            "job start": 2,
            "start_job call": 2,
        }
    )
    assert dummy_stac_api.items == {
        "collection-123": {
            "item-2024": dirty_equals.IsPartialDict(
                {
                    "type": "Feature",
                    "id": "item-2024",
                    "properties": dirty_equals.IsPartialDict(
                        {
                            "year": 2024,
                            "id": "job-000",
                            "status": "finished",
                            "backend_name": "foo",
                        }
                    ),
                }
            ),
            "item-2025": dirty_equals.IsPartialDict(
                {
                    "type": "Feature",
                    "id": "item-2025",
                    "properties": dirty_equals.IsPartialDict(
                        {
                            "year": 2025,
                            "id": "job-001",
                            "status": "finished",
                            "backend_name": "foo",
                        }
                    ),
                }
            ),
        }
    }
