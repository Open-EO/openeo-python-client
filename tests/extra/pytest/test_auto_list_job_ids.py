from pathlib import Path

import pytest

pytest_plugins = "pytester"


@pytest.fixture
def pytester(pytester: pytest.Pytester) -> pytest.Pytester:
    """Pytester preloaded with the plugin under test and some generic utilities"""
    conftest = (Path(__file__).parent / "dummy_conftest.py").read_text()
    pytester.makeconftest(conftest)
    return pytester


class TestAutoListJobIds:
    def test_without_jobs(self, pytester: pytest.Pytester):
        """Tests without jobs or auto_list_job_ids usage keep working."""
        pytester.makepyfile(
            """
            def test_other():
                assert True

            def test_dummy(auto_list_job_ids):
                assert True
            """
        )
        result = pytester.runpytest()
        result.assert_outcomes(passed=2)

    def test_job_ids_listed_on_failure(self, pytester: pytest.Pytester):
        """
        When the test fails, the job id should be listed in the terminal report
        """
        pytester.makepyfile(
            """
            def test_create_job_but_then_fail(connection):
                pg = {"add35": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
                connection.create_job(pg)
                x = 4 / 0
            """
        )
        result = pytester.runpytest()
        result.assert_outcomes(failed=1)
        result.stdout.fnmatch_lines(
            [
                "*Jobs created during this test*",
                "job-3535",
            ]
        )

    @pytest.mark.parametrize(
        ["pytest_args", "expected"],
        [
            ((), False),
            (("-rA",), True),
        ],
    )
    def test_job_ids_listed_on_passing_test(self, pytester: pytest.Pytester, pytest_args, expected):
        """
        On a passing tests, job ids can also be shown with appropriate pytest options
        """
        pytester.makepyfile(
            """
            def test_create_job(connection):
                pg = {"add35": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
                connection.create_job(pg)
            """
        )

        result_default = pytester.runpytest(*pytest_args)
        result_default.assert_outcomes(passed=1)
        assert ("job-3535" in result_default.stdout.str()) == expected

    def test_isolated_histories(self, pytester: pytest.Pytester):
        """Job ids from one test must not leak into another test's report."""
        pytester.makepyfile(
            """
            def test_35(connection):
                pg = {"add35": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
                connection.create_job(pg)
                x = 35 / 0

            def test_58(connection):
                pg = {"add58": {"process_id": "add", "arguments": {"x": 5, "y": 8}, "result": True}}
                connection.create_job(pg)
                x = 58 / 0
            """
        )
        result = pytester.runpytest()
        result.assert_outcomes(failed=2)
        result.stdout.fnmatch_lines(
            [
                # TODO: this might need hardening against plugins that randomize test order
                "*test_35*",
                "*Jobs created during this test*",
                "job-3535",
                "*test_58*",
                "*Jobs created during this test*",
                "job-5858",
            ]
        )
