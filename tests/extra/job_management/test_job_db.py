
import re
import geopandas

# TODO: can we avoid using httpretty?
#   We need it for testing the resilience, which uses an HTTPadapter with Retry
#   but requests-mock also uses an HTTPAdapter for the mocking and basically
#   erases the HTTPAdapter we have set up.
#   httpretty avoids this specific problem because it mocks at the socket level,
#   But I would rather not have two dependencies with almost the same goal.
import pandas as pd
import pytest
import shapely.geometry

from openeo.extra.job_management._job_db import (
    CsvJobDatabase,
    ParquetJobDatabase,
    create_job_db,
    get_job_db,
)

from openeo.utils.version import ComparableVersion

JOB_DB_DF_BASICS = pd.DataFrame(
    {
        "numbers": [3, 2, 1],
        "names": ["apple", "banana", "coconut"],
    }
)
JOB_DB_GDF_WITH_GEOMETRY = geopandas.GeoDataFrame(
    {
        "numbers": [11, 22],
        "geometry": [shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 1)],
    },
)
JOB_DB_DF_WITH_GEOJSON_STRING = pd.DataFrame(
    {
        "numbers": [11, 22],
        "geometry": ['{"type":"Point","coordinates":[1,2]}', '{"type":"Point","coordinates":[1,2]}'],
    }
)


class TestFullDataFrameJobDatabase:
    @pytest.mark.parametrize("db_class", [CsvJobDatabase, ParquetJobDatabase])
    def test_initialize_from_df(self, tmp_path, db_class):
        orig_df = pd.DataFrame({"some_number": [3, 2, 1]})
        path = tmp_path / "jobs.db"

        db = db_class(path)
        assert not path.exists()
        db.initialize_from_df(orig_df)
        assert path.exists()

        # Check persisted CSV
        assert path.exists()
        expected_columns = {
            "some_number",
            "status",
            "id",
            "start_time",
            "running_start_time",
            "cpu",
            "memory",
            "duration",
            "backend_name",
            "costs",
        }

        actual_columns = set(db_class(path).read().columns)
        assert actual_columns == expected_columns

    @pytest.mark.parametrize("db_class", [CsvJobDatabase, ParquetJobDatabase])
    def test_initialize_from_df_on_exists_error(self, tmp_path, db_class):
        df = pd.DataFrame({"some_number": [3, 2, 1]})
        path = tmp_path / "jobs.csv"
        _ = db_class(path).initialize_from_df(df, on_exists="error")
        assert path.exists()

        with pytest.raises(FileExistsError, match="Job database.* already exists"):
            _ = db_class(path).initialize_from_df(df, on_exists="error")

        assert set(db_class(path).read()["some_number"]) == {1, 2, 3}

    @pytest.mark.parametrize("db_class", [CsvJobDatabase, ParquetJobDatabase])
    def test_initialize_from_df_on_exists_skip(self, tmp_path, db_class):
        path = tmp_path / "jobs.db"

        db = db_class(path).initialize_from_df(
            pd.DataFrame({"some_number": [3, 2, 1]}),
            on_exists="skip",
        )
        assert set(db.read()["some_number"]) == {1, 2, 3}

        db = db_class(path).initialize_from_df(
            pd.DataFrame({"some_number": [444, 555, 666]}),
            on_exists="skip",
        )
        assert set(db.read()["some_number"]) == {1, 2, 3}

    @pytest.mark.parametrize("db_class", [CsvJobDatabase, ParquetJobDatabase])
    def test_count_by_status(self, tmp_path, db_class):
        path = tmp_path / "jobs.db"

        db = db_class(path).initialize_from_df(
            pd.DataFrame(
                {
                    "status": [
                        "not_started",
                        "created",
                        "queued",
                        "queued",
                        "queued",
                        "running",
                        "running",
                        "finished",
                        "finished",
                        "error",
                    ]
                }
            )
        )
        assert db.count_by_status(statuses=["not_started"]) == {"not_started": 1}
        assert db.count_by_status(statuses=("not_started", "running")) == {"not_started": 1, "running": 2}
        assert db.count_by_status(statuses={"finished", "error"}) == {"error": 1, "finished": 2}

        # All statuses by default
        assert db.count_by_status() == {
            "created": 1,
            "error": 1,
            "finished": 2,
            "not_started": 1,
            "queued": 3,
            "running": 2,
        }


class TestCsvJobDatabase:
    def test_repr(self, tmp_path):
        path = tmp_path / "db.csv"
        db = CsvJobDatabase(path)
        assert re.match(r"CsvJobDatabase\('[^']+\.csv'\)", repr(db))
        assert re.match(r"CsvJobDatabase\('[^']+\.csv'\)", str(db))

    def test_read_wkt(self, tmp_path):
        wkt_df = pd.DataFrame(
            {
                "value": ["wkt"],
                "geometry": ["POINT (30 10)"],
            }
        )
        path = tmp_path / "jobs.csv"
        wkt_df.to_csv(path, index=False)
        df = CsvJobDatabase(path).read()
        assert isinstance(df.geometry[0], shapely.geometry.Point)

    def test_read_non_wkt(self, tmp_path):
        non_wkt_df = pd.DataFrame(
            {
                "value": ["non_wkt"],
                "geometry": ["this is no WKT"],
            }
        )
        path = tmp_path / "jobs.csv"
        non_wkt_df.to_csv(path, index=False)
        df = CsvJobDatabase(path).read()
        assert isinstance(df.geometry[0], str)

    @pytest.mark.parametrize(
        ["orig"],
        [
            pytest.param(JOB_DB_DF_BASICS, id="pandas basics"),
            pytest.param(JOB_DB_GDF_WITH_GEOMETRY, id="geopandas with geometry"),
            pytest.param(JOB_DB_DF_WITH_GEOJSON_STRING, id="pandas with geojson string as geometry"),
        ],
    )
    def test_persist_and_read(self, tmp_path, orig: pd.DataFrame):
        path = tmp_path / "jobs.parquet"
        CsvJobDatabase(path).persist(orig)
        assert path.exists()

        loaded = CsvJobDatabase(path).read()
        assert loaded.dtypes.to_dict() == orig.dtypes.to_dict()
        assert loaded.equals(orig)
        assert type(orig) is type(loaded)

    @pytest.mark.parametrize(
        ["orig"],
        [
            pytest.param(JOB_DB_DF_BASICS, id="pandas basics"),
            pytest.param(JOB_DB_GDF_WITH_GEOMETRY, id="geopandas with geometry"),
            pytest.param(JOB_DB_DF_WITH_GEOJSON_STRING, id="pandas with geojson string as geometry"),
        ],
    )
    def test_partial_read_write(self, tmp_path, orig: pd.DataFrame):
        path = tmp_path / "jobs.csv"

        required_with_default = [
            ("status", "not_started"),
            ("id", None),
            ("start_time", None),
        ]
        new_columns = {col: val for (col, val) in required_with_default if col not in orig.columns}
        orig = orig.assign(**new_columns)

        db = CsvJobDatabase(path)
        db.persist(orig)
        assert path.exists()

        loaded = db.get_by_status(statuses=["not_started"], max=2)
        assert db.count_by_status(statuses=["not_started"])["not_started"] > 1

        assert len(loaded) == 2
        loaded.loc[0, "status"] = "running"
        loaded.loc[1, "status"] = "error"
        db.persist(loaded)
        assert db.count_by_status(statuses=["error"])["error"] == 1

        all = db.read()
        assert len(all) == len(orig)
        assert all.loc[0, "status"] == "running"
        assert all.loc[1, "status"] == "error"
        if len(all) > 2:
            assert all.loc[2, "status"] == "not_started"
        print(loaded.index)

    def test_initialize_from_df(self, tmp_path):
        orig_df = pd.DataFrame({"some_number": [3, 2, 1]})
        path = tmp_path / "jobs.csv"

        # Initialize the CSV from the dataframe
        _ = CsvJobDatabase(path).initialize_from_df(orig_df)

        # Check persisted CSV
        assert path.exists()
        expected_columns = {
            "some_number",
            "status",
            "id",
            "start_time",
            "running_start_time",
            "cpu",
            "memory",
            "duration",
            "backend_name",
            "costs",
        }

        # Raw file content check
        raw_columns = set(path.read_text().split("\n")[0].split(","))
        # Higher level read
        read_columns = set(CsvJobDatabase(path).read().columns)

        assert raw_columns == expected_columns
        assert read_columns == expected_columns

    def test_initialize_from_df_on_exists_error(self, tmp_path):
        orig_df = pd.DataFrame({"some_number": [3, 2, 1]})
        path = tmp_path / "jobs.csv"
        _ = CsvJobDatabase(path).initialize_from_df(orig_df, on_exists="error")
        with pytest.raises(FileExistsError, match="Job database.* already exists"):
            _ = CsvJobDatabase(path).initialize_from_df(orig_df, on_exists="error")

    def test_initialize_from_df_on_exists_skip(self, tmp_path):
        path = tmp_path / "jobs.csv"

        db = CsvJobDatabase(path).initialize_from_df(
            pd.DataFrame({"some_number": [3, 2, 1]}),
            on_exists="skip",
        )
        assert set(db.read()["some_number"]) == {1, 2, 3}

        db = CsvJobDatabase(path).initialize_from_df(
            pd.DataFrame({"some_number": [444, 555, 666]}),
            on_exists="skip",
        )
        assert set(db.read()["some_number"]) == {1, 2, 3}

    @pytest.mark.skipif(
        ComparableVersion(geopandas.__version__) < "0.14",
        reason="This issue has no workaround with geopandas < 0.14 (highest available version on Python 3.8 is 0.13.2)",
    )
    def test_read_with_crs_column(self, tmp_path):
        """
        Having a column named "crs" can cause obscure error messages when creating a GeoPandas dataframe
        https://github.com/Open-EO/openeo-python-client/issues/714
        """
        source_df = pd.DataFrame(
            {
                "crs": [1234],
                "geometry": ["Point(2 3)"],
            }
        )
        path = tmp_path / "jobs.csv"
        source_df.to_csv(path, index=False)
        result_df = CsvJobDatabase(path).read()
        assert isinstance(result_df, geopandas.GeoDataFrame)
        assert result_df.to_dict(orient="list") == {
            "crs": [1234],
            "geometry": [shapely.geometry.Point(2, 3)],
        }


class TestParquetJobDatabase:
    def test_repr(self, tmp_path):
        path = tmp_path / "db.pq"
        db = ParquetJobDatabase(path)
        assert re.match(r"ParquetJobDatabase\('[^']+\.pq'\)", repr(db))
        assert re.match(r"ParquetJobDatabase\('[^']+\.pq'\)", str(db))

    @pytest.mark.parametrize(
        ["orig"],
        [
            pytest.param(JOB_DB_DF_BASICS, id="pandas basics"),
            pytest.param(JOB_DB_GDF_WITH_GEOMETRY, id="geopandas with geometry"),
            pytest.param(JOB_DB_DF_WITH_GEOJSON_STRING, id="pandas with geojson string as geometry"),
        ],
    )
    def test_persist_and_read(self, tmp_path, orig: pd.DataFrame):
        path = tmp_path / "jobs.parquet"
        ParquetJobDatabase(path).persist(orig)
        assert path.exists()

        loaded = ParquetJobDatabase(path).read()
        assert loaded.dtypes.to_dict() == orig.dtypes.to_dict()
        assert loaded.equals(orig)
        assert type(orig) is type(loaded)

    def test_initialize_from_df(self, tmp_path):
        orig_df = pd.DataFrame({"some_number": [3, 2, 1]})
        path = tmp_path / "jobs.parquet"

        # Initialize the CSV from the dataframe
        _ = ParquetJobDatabase(path).initialize_from_df(orig_df)

        # Check persisted CSV
        assert path.exists()
        expected_columns = {
            "some_number",
            "status",
            "id",
            "start_time",
            "running_start_time",
            "cpu",
            "memory",
            "duration",
            "backend_name",
            "costs",
        }

        df_from_disk = ParquetJobDatabase(path).read()
        assert set(df_from_disk.columns) == expected_columns


@pytest.mark.parametrize(
    ["filename", "expected"],
    [
        ("jobz.csv", CsvJobDatabase),
        ("jobz.parquet", ParquetJobDatabase),
    ],
)
def test_get_job_db(tmp_path, filename, expected):
    path = tmp_path / filename
    db = get_job_db(path)
    assert isinstance(db, expected)
    assert not path.exists()


@pytest.mark.parametrize(
    ["filename", "expected"],
    [
        ("jobz.csv", CsvJobDatabase),
        ("jobz.parquet", ParquetJobDatabase),
    ],
)
def test_create_job_db(tmp_path, filename, expected):
    df = pd.DataFrame({"year": [2023, 2024]})
    path = tmp_path / filename
    db = create_job_db(path=path, df=df)
    assert isinstance(db, expected)
    assert path.exists()