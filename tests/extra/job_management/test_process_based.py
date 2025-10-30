import copy
from unittest import mock

import dirty_equals
import geopandas
import pandas as pd
import pytest

import openeo
from openeo import BatchJob
from openeo.extra.job_management import (
    CsvJobDatabase,
    MultiBackendJobManager,
    ParquetJobDatabase,
)
from openeo.extra.job_management.process_based import ProcessBasedJobCreator
from openeo.rest._testing import OPENEO_BACKEND, DummyBackend, build_capabilities


@pytest.fixture
def sleep_mock():
    with mock.patch("time.sleep") as sleep:
        yield sleep


@pytest.fixture
def con(requests_mock) -> openeo.Connection:
    requests_mock.get(OPENEO_BACKEND, json=build_capabilities(api_version="1.2.0", udp=True))
    con = openeo.Connection(OPENEO_BACKEND)
    return con


class TestProcessBasedJobCreator:
    @pytest.fixture
    def dummy_backend(self, requests_mock, con) -> DummyBackend:
        dummy = DummyBackend(requests_mock=requests_mock, connection=con)
        dummy.setup_simple_job_status_flow(queued=2, running=3, final="finished")
        return dummy

    PG_3PLUS5 = {
        "id": "3plus5",
        "process_graph": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True},
    }
    PG_INCREMENT = {
        "id": "increment",
        "parameters": [
            {"name": "data", "description": "data", "schema": {"type": "number"}},
            {
                "name": "increment",
                "description": "increment",
                "schema": {"type": "number"},
                "optional": True,
                "default": 1,
            },
        ],
        "process_graph": {
            "process_id": "add",
            "arguments": {"x": {"from_parameter": "data"}, "y": {"from_parameter": "increment"}},
            "result": True,
        },
    }
    PG_OFFSET_POLYGON = {
        "id": "offset_polygon",
        "parameters": [
            {"name": "data", "description": "data", "schema": {"type": "number"}},
            {
                "name": "polygons",
                "description": "polygons",
                "schema": {
                    "title": "GeoJSON",
                    "type": "object",
                    "subtype": "geojson",
                },
            },
            {
                "name": "offset",
                "description": "Offset",
                "schema": {"type": "number"},
                "optional": True,
                "default": 0,
            },
        ],
    }

    @pytest.fixture(autouse=True)
    def remote_process_definitions(self, requests_mock) -> dict:
        mocks = {}
        processes = [self.PG_3PLUS5, self.PG_INCREMENT, self.PG_OFFSET_POLYGON]
        mocks["_all"] = requests_mock.get("https://remote.test/_all", json={"processes": processes, "links": []})
        for pg in processes:
            process_id = pg["id"]
            mocks[process_id] = requests_mock.get(f"https://remote.test/{process_id}.json", json=pg)
        return mocks

    def test_minimal(self, con, dummy_backend, remote_process_definitions):
        """Bare minimum: just start a job, no parameters/arguments"""
        job_factory = ProcessBasedJobCreator(process_id="3plus5", namespace="https://remote.test/3plus5.json")

        job = job_factory.start_job(row=pd.Series({"foo": 123}), connection=con)
        assert isinstance(job, BatchJob)
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "3plus51": {
                        "process_id": "3plus5",
                        "namespace": "https://remote.test/3plus5.json",
                        "arguments": {},
                        "result": True,
                    }
                },
                "status": "created",
                "title": "Process '3plus5' with {}",
                "description": "Process '3plus5' (namespace https://remote.test/3plus5.json) with {}",
            }
        }

        assert remote_process_definitions["3plus5"].call_count == 1

    def test_basic(self, con, dummy_backend, remote_process_definitions):
        """Basic parameterized UDP job generation"""
        dummy_backend.extra_job_metadata_fields = ["title", "description"]
        job_factory = ProcessBasedJobCreator(process_id="increment", namespace="https://remote.test/increment.json")

        job = job_factory.start_job(row=pd.Series({"data": 123}), connection=con)
        assert isinstance(job, BatchJob)
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 123, "increment": 1},
                        "result": True,
                    }
                },
                "status": "created",
                "title": "Process 'increment' with {'data': 123, 'increment': 1}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 123, 'increment': 1}",
            }
        }
        assert remote_process_definitions["increment"].call_count == 1

    @pytest.mark.parametrize(
        ["parameter_defaults", "row", "expected_arguments"],
        [
            (None, {"data": 123}, {"data": 123, "increment": 1}),
            (None, {"data": 123, "increment": 5}, {"data": 123, "increment": 5}),
            ({"increment": 5}, {"data": 123}, {"data": 123, "increment": 5}),
            ({"increment": 5}, {"data": 123, "increment": 1000}, {"data": 123, "increment": 1000}),
        ],
    )
    def test_basic_parameterization(self, con, dummy_backend, parameter_defaults, row, expected_arguments):
        """Basic parameterized UDP job generation"""
        job_factory = ProcessBasedJobCreator(
            process_id="increment",
            namespace="https://remote.test/increment.json",
            parameter_defaults=parameter_defaults,
        )

        job = job_factory.start_job(row=pd.Series(row), connection=con)
        assert isinstance(job, BatchJob)
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": expected_arguments,
                        "result": True,
                    }
                },
                "status": "created",
                "title": dirty_equals.IsStr(regex="Process 'increment' with .*"),
                "description": dirty_equals.IsStr(regex="Process 'increment' .*"),
            }
        }

    @pytest.mark.parametrize(
        ["process_id", "namespace", "expected"],
        [
            (
                # Classic UDP reference
                "3plus5",
                None,
                {"process_id": "3plus5"},
            ),
            (
                # Remote process definition (with "redundant" process_id)
                "3plus5",
                "https://remote.test/3plus5.json",
                {"process_id": "3plus5", "namespace": "https://remote.test/3plus5.json"},
            ),
            (
                # Remote process definition with just namespace (process_id should be inferred from that)
                None,
                "https://remote.test/3plus5.json",
                {"process_id": "3plus5", "namespace": "https://remote.test/3plus5.json"},
            ),
            (
                # Remote process definition from listing
                "3plus5",
                "https://remote.test/_all",
                {"process_id": "3plus5", "namespace": "https://remote.test/_all"},
            ),
        ],
    )
    def test_process_references_in_constructor(
        self, con, requests_mock, dummy_backend, remote_process_definitions, process_id, namespace, expected
    ):
        """Various ways to provide process references in the constructor"""

        # Register personal UDP
        requests_mock.get(con.build_url("/process_graphs/3plus5"), json=self.PG_3PLUS5)

        job_factory = ProcessBasedJobCreator(process_id=process_id, namespace=namespace)

        job = job_factory.start_job(row=pd.Series({"foo": 123}), connection=con)
        assert isinstance(job, BatchJob)
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {"3plus51": {**expected, "arguments": {}, "result": True}},
                "status": "created",
                "title": "Process '3plus5' with {}",
                "description": f"Process '3plus5' (namespace {namespace}) with {{}}",
            }
        }

    def test_no_process_id_nor_namespace(self):
        with pytest.raises(ValueError, match="At least one of `process_id` and `namespace` should be provided"):
            _ = ProcessBasedJobCreator()

    @pytest.fixture
    def job_manager(self, tmp_path, dummy_backend) -> MultiBackendJobManager:
        job_manager = MultiBackendJobManager(root_dir=tmp_path / "job_mgr_root")
        job_manager.add_backend("dummy", connection=dummy_backend.connection, parallel_jobs=1)
        return job_manager

    def test_with_job_manager_remote_basic(
        self, tmp_path, requests_mock, dummy_backend, job_manager, sleep_mock, remote_process_definitions
    ):
        job_starter = ProcessBasedJobCreator(
            process_id="increment",
            namespace="https://remote.test/increment.json",
            parameter_defaults={"increment": 5},
        )

        df = pd.DataFrame({"data": [1, 2, 3]})
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=1),
                "start_job call": 3,
                "job start": 3,
                "job started running": 3,
                "job finished": 3,
            }
        )
        assert set(job_db.read().status) == {"finished"}

        # Verify caching of HTTP request of remote process definition
        assert remote_process_definitions["increment"].call_count == 1

        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 1, "increment": 5},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 1, 'increment': 5}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 1, 'increment': 5}",
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 2, "increment": 5},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 2, 'increment': 5}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 2, 'increment': 5}",
            },
            "job-002": {
                "job_id": "job-002",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 3, "increment": 5},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 3, 'increment': 5}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 3, 'increment': 5}",
            },
        }

    @pytest.mark.parametrize(
        ["parameter_defaults", "df_data", "expected_arguments"],
        [
            (
                {"increment": 5},
                {"data": [1, 2, 3]},
                {
                    "job-000": {"data": 1, "increment": 5},
                    "job-001": {"data": 2, "increment": 5},
                    "job-002": {"data": 3, "increment": 5},
                },
            ),
            (
                None,
                {"data": [1, 2, 3], "increment": [44, 55, 66]},
                {
                    "job-000": {"data": 1, "increment": 44},
                    "job-001": {"data": 2, "increment": 55},
                    "job-002": {"data": 3, "increment": 66},
                },
            ),
            (
                {"increment": 5555},
                {"data": [1, 2, 3], "increment": [44, 55, 66]},
                {
                    "job-000": {"data": 1, "increment": 44},
                    "job-001": {"data": 2, "increment": 55},
                    "job-002": {"data": 3, "increment": 66},
                },
            ),
        ],
    )
    def test_with_job_manager_remote_parameter_handling(
        self,
        tmp_path,
        requests_mock,
        dummy_backend,
        job_manager,
        sleep_mock,
        parameter_defaults,
        df_data,
        expected_arguments,
    ):
        job_starter = ProcessBasedJobCreator(
            process_id="increment",
            namespace="https://remote.test/increment.json",
            parameter_defaults=parameter_defaults,
        )

        df = pd.DataFrame(df_data)
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=1),
                "start_job call": 3,
                "job start": 3,
                "job finished": 3,
            }
        )
        assert set(job_db.read().status) == {"finished"}

        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": expected_arguments["job-000"],
                        "result": True,
                    }
                },
                "status": "finished",
                "title": dirty_equals.IsStr(regex="Process 'increment' with .*"),
                "description": dirty_equals.IsStr(regex="Process 'increment'.*"),
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": expected_arguments["job-001"],
                        "result": True,
                    }
                },
                "status": "finished",
                "title": dirty_equals.IsStr(regex="Process 'increment' with .*"),
                "description": dirty_equals.IsStr(regex="Process 'increment'.*"),
            },
            "job-002": {
                "job_id": "job-002",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": expected_arguments["job-002"],
                        "result": True,
                    }
                },
                "status": "finished",
                "title": dirty_equals.IsStr(regex="Process 'increment' with .*"),
                "description": dirty_equals.IsStr(regex="Process 'increment'.*"),
            },
        }

    def test_with_job_manager_remote_geometry(self, tmp_path, requests_mock, dummy_backend, job_manager, sleep_mock):
        job_starter = ProcessBasedJobCreator(
            process_id="offset_polygon",
            namespace="https://remote.test/offset_polygon.json",
            parameter_defaults={"data": 123},
        )

        df = geopandas.GeoDataFrame.from_features(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "id": "one",
                        "properties": {"offset": 11},
                        "geometry": {"type": "Point", "coordinates": (1.0, 2.0)},
                    },
                    {
                        "type": "Feature",
                        "id": "two",
                        "properties": {"offset": 22},
                        "geometry": {"type": "Point", "coordinates": (3.0, 4.0)},
                    },
                ],
            }
        )

        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=1),
                "start_job call": 2,
                "job start": 2,
                "job finished": 2,
            }
        )
        assert set(job_db.read().status) == {"finished"}

        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "offsetpolygon1": {
                        "process_id": "offset_polygon",
                        "namespace": "https://remote.test/offset_polygon.json",
                        "arguments": {
                            "data": 123,
                            "polygons": {"type": "Point", "coordinates": [1.0, 2.0]},
                            "offset": 11,
                        },
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'offset_polygon' with {'data': 123, 'polygons': {'type': 'Point', 'coordinates': (1...",
                "description": "Process 'offset_polygon' (namespace https://remote.test/offset_polygon.json) with {'data': 123, 'polygons': {'type': 'Point', 'coordinates': (1.0, 2.0)}, 'offset': 11}",
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "offsetpolygon1": {
                        "process_id": "offset_polygon",
                        "namespace": "https://remote.test/offset_polygon.json",
                        "arguments": {
                            "data": 123,
                            "polygons": {"type": "Point", "coordinates": [3.0, 4.0]},
                            "offset": 22,
                        },
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'offset_polygon' with {'data': 123, 'polygons': {'type': 'Point', 'coordinates': (3...",
                "description": "Process 'offset_polygon' (namespace https://remote.test/offset_polygon.json) with {'data': 123, 'polygons': {'type': 'Point', 'coordinates': (3.0, 4.0)}, 'offset': 22}",
            },
        }

    @pytest.mark.parametrize(
        ["db_class"],
        [
            (CsvJobDatabase,),
            (ParquetJobDatabase,),
        ],
    )
    def test_with_job_manager_remote_geometry_after_resume(
        self, tmp_path, requests_mock, dummy_backend, job_manager, sleep_mock, db_class
    ):
        """Test if geometry handling works properly after resuming from CSV serialized job db."""
        job_starter = ProcessBasedJobCreator(
            process_id="offset_polygon",
            namespace="https://remote.test/offset_polygon.json",
            parameter_defaults={"data": 123},
        )

        df = geopandas.GeoDataFrame.from_features(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "id": "one",
                        "properties": {"offset": 11},
                        "geometry": {"type": "Point", "coordinates": (1.0, 2.0)},
                    },
                    {
                        "type": "Feature",
                        "id": "two",
                        "properties": {"offset": 22},
                        "geometry": {"type": "Point", "coordinates": (3.0, 4.0)},
                    },
                ],
            }
        )

        # Persist the job db to CSV/Parquet/...
        job_db_path = tmp_path / "jobs.db"
        _ = db_class(job_db_path).initialize_from_df(df)
        assert job_db_path.exists()

        # Resume from persisted job db
        job_db = db_class(job_db_path)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "sleep": dirty_equals.IsInt(gt=1),
                "start_job call": 2,
                "job start": 2,
                "job finished": 2,
            }
        )
        assert set(job_db.read().status) == {"finished"}

        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "offsetpolygon1": {
                        "process_id": "offset_polygon",
                        "namespace": "https://remote.test/offset_polygon.json",
                        "arguments": {
                            "data": 123,
                            "polygons": {"type": "Point", "coordinates": [1.0, 2.0]},
                            "offset": 11,
                        },
                        "result": True,
                    }
                },
                "status": "finished",
                "title": dirty_equals.IsStr(regex="Process 'offset_polygon' with.*"),
                "description": dirty_equals.IsStr(regex="Process 'offset_polygon' .*"),
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "offsetpolygon1": {
                        "process_id": "offset_polygon",
                        "namespace": "https://remote.test/offset_polygon.json",
                        "arguments": {
                            "data": 123,
                            "polygons": {"type": "Point", "coordinates": [3.0, 4.0]},
                            "offset": 22,
                        },
                        "result": True,
                    }
                },
                "status": "finished",
                "title": dirty_equals.IsStr(regex="Process 'offset_polygon' with.*"),
                "description": dirty_equals.IsStr(regex="Process 'offset_polygon' .*"),
            },
        }

    def test_with_job_manager_udp_basic(
        self, tmp_path, requests_mock, con, dummy_backend, job_manager, sleep_mock, remote_process_definitions
    ):
        # make deep copy
        udp = copy.deepcopy(self.PG_INCREMENT)
        # Register personal UDP
        increment_udp_mock = requests_mock.get(con.build_url("/process_graphs/increment"), json=udp)

        job_starter = ProcessBasedJobCreator(
            process_id="increment",
            # No namespace to trigger personal UDP mode
            namespace=None,
            parameter_defaults={"increment": 5},
        )
        assert increment_udp_mock.call_count == 0

        df = pd.DataFrame({"data": [3, 5]})
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "start_job call": 2,
                "job finished": 2,
            }
        )
        assert increment_udp_mock.call_count == 2
        assert set(job_db.read().status) == {"finished"}

        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "arguments": {"data": 3, "increment": 5},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 3, 'increment': 5}",
                "description": "Process 'increment' (namespace None) with {'data': 3, 'increment': 5}",
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "arguments": {"data": 5, "increment": 5},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 5, 'increment': 5}",
                "description": "Process 'increment' (namespace None) with {'data': 5, 'increment': 5}",
            },
        }

    def test_with_job_manager_parameter_column_map(
        self, tmp_path, requests_mock, dummy_backend, job_manager, sleep_mock, remote_process_definitions
    ):
        job_starter = ProcessBasedJobCreator(
            process_id="increment",
            namespace="https://remote.test/increment.json",
            parameter_column_map={"data": "numberzzz", "increment": "add_thiz"},
        )

        df = pd.DataFrame(
            {
                "data": [1, 2],
                "increment": [-1, -2],
                "numberzzz": [3, 5],
                "add_thiz": [100, 200],
            }
        )
        job_db = CsvJobDatabase(tmp_path / "jobs.csv").initialize_from_df(df)

        stats = job_manager.run_jobs(job_db=job_db, start_job=job_starter)
        assert stats == dirty_equals.IsPartialDict(
            {
                "start_job call": 2,
                "job finished": 2,
            }
        )
        assert dummy_backend.batch_jobs == {
            "job-000": {
                "job_id": "job-000",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 3, "increment": 100},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 3, 'increment': 100}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 3, 'increment': 100}",
            },
            "job-001": {
                "job_id": "job-001",
                "pg": {
                    "increment1": {
                        "process_id": "increment",
                        "namespace": "https://remote.test/increment.json",
                        "arguments": {"data": 5, "increment": 200},
                        "result": True,
                    }
                },
                "status": "finished",
                "title": "Process 'increment' with {'data': 5, 'increment': 200}",
                "description": "Process 'increment' (namespace https://remote.test/increment.json) with {'data': 5, 'increment': 200}",
            },
        }
