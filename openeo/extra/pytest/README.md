
# openEO oriented pytest plugins

> [!WARNING]
> These plugins are still in an experimental stage

## Dependencies

While the general `openeo` package has no runtime dependency on pytest,
these plugins obviously do.


## `auto_list_job_ids`

Plugin to automatically list job ids that are created during a test
in the test report (e.g. terminal output, HTML report, JUnit XML report).

Usage instructions:

Enable the plugin in your `conftest.py`:

```python
pytest_plugins = ["openeo.extra.pytest.auto_list_job_ids"]
```

Using the `auto_list_job_ids` fixture provided by the plugin,
inject the necessary instrumentation in the `Connection` object,
after creation in your tests functions, or more commonly,
from your connection fixture:

```python
@pytest.fixture
def connection(auto_list_job_ids):
    con = openeo.connect("https://openeo.example.com/")
    auto_list_job_ids(con)
    return con
```

Failed tests will get an additional section listing all jobs
that were created during the test (regardless of current/final status):

```
=================================== FAILURES ===================================
________________________________ test_something ________________________________
...
E       ZeroDivisionError: division by zero
...
------------------------ Jobs created during this test -------------------------
job-123
```

To also get this listing on successful tests:
enable summary info for all outcomes using pytest's `-rA` option.
