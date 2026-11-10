import re
import threading

import geopandas
import pandas as pd
import pytest
import shapely.geometry

from openeo.extra.job_management.sqlite_job_db import SqliteJobDatabase

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


class TestSqliteJobDatabase:
    def test_repr(self, tmp_path):
        db = SqliteJobDatabase(tmp_path / "db.sqlite")
        assert re.match(r"SqliteJobDatabase\('[^']+\.sqlite'\)", repr(db))

    @pytest.mark.parametrize(
        ["orig"],
        [
            pytest.param(JOB_DB_DF_BASICS, id="pandas basics"),
            pytest.param(JOB_DB_GDF_WITH_GEOMETRY, id="geopandas with geometry"),
            pytest.param(JOB_DB_DF_WITH_GEOJSON_STRING, id="pandas with geojson string as geometry"),
            pytest.param(
                pd.DataFrame({"name": ["a", None], "value": [1.5, float("nan")]}), id="pandas with missing values"
            ),
        ],
    )
    def test_persist_and_read(self, tmp_path, orig: pd.DataFrame):
        path = tmp_path / "jobs.sqlite"
        SqliteJobDatabase(path).persist(orig)
        assert path.exists()

        loaded = SqliteJobDatabase(path).read()
        assert loaded.dtypes.to_dict() == orig.dtypes.to_dict()
        assert loaded.equals(orig)
        assert type(orig) is type(loaded)

    def test_partial_update(self, tmp_path):
        db = SqliteJobDatabase(tmp_path / "jobs.sqlite").initialize_from_df(pd.DataFrame({"year": [2021, 2022, 2023]}))

        loaded = db.get_by_status(statuses=["not_started"], max=2)
        assert list(loaded.index) == [0, 1]
        loaded.loc[0, "status"] = "running"
        loaded.loc[1, "status"] = "error"
        db.persist(loaded.loc[[0, 1]])

        assert db.count_by_status() == {"running": 1, "error": 1, "not_started": 1}
        assert list(db.read()["status"]) == ["running", "error", "not_started"]
        assert list(db.get_by_indices([2, 0]).sort_index()["year"]) == [2021, 2023]

    def test_persist_unsupported_types_as_str(self, tmp_path):
        df = pd.DataFrame({"date": [pd.Timestamp("2024-01-02")], "bands": [["B02", "B03"]]}, index=["a"])
        db = SqliteJobDatabase(tmp_path / "jobs.sqlite")
        db.persist(df)
        assert db.read().to_dict(orient="index") == {"a": {"date": "2024-01-02 00:00:00", "bands": "['B02', 'B03']"}}

    def test_persist_reserved_column(self, tmp_path):
        path = tmp_path / "jobs.sqlite"
        with pytest.raises(ValueError, match="reserved"):
            SqliteJobDatabase(path).persist(pd.DataFrame({"_job_db_index": [1]}))
        assert not path.exists()

    def test_update_rollback_includes_schema(self, tmp_path):
        class Unconvertible:
            def __str__(self):
                raise RuntimeError("nope")

        db = SqliteJobDatabase(tmp_path / "jobs.sqlite").initialize_from_df(pd.DataFrame({"year": [2021, 2022]}))
        with pytest.raises(RuntimeError, match="nope"):
            db.update({0: {"status": "queued", "new": "x"}, 1: {"bad": Unconvertible()}})

        df = db.read()
        assert "new" not in df.columns
        assert list(df["status"]) == ["not_started", "not_started"]

    def test_concurrent_updates_adding_columns(self, tmp_path):
        path = tmp_path / "jobs.sqlite"
        SqliteJobDatabase(path).initialize_from_df(pd.DataFrame({"year": range(8)}))
        errors = []

        def work(i):
            try:
                SqliteJobDatabase(path).update({i: {"new": f"x{i}"}})
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=work, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert list(SqliteJobDatabase(path).read()["new"]) == [f"x{i}" for i in range(8)]
