===============================
Miscellaneous tips and tricks
===============================


Execute a process graph directly from raw JSON
-----------------------------------------------

When you have a process graph in JSON format, as a string, a local file or a URL,
you can execute/download it without converting it do a DataCube first.
Just pass the string, path or URL directly to
:py:meth:`Connection.download() <openeo.rest.connection.Connection.download>`,
:py:meth:`Connection.execute() <openeo.rest.connection.Connection.execute>` or
:py:meth:`Connection.create_job() <openeo.rest.connection.Connection.create_job>`.
For example:

.. code-block:: python

    # `execute` with raw JSON string
    connection.execute("""
        {
            "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": true}
        }
    """)

    # `download` with local path to JSON file
    connection.download("path/to/my-process-graph.json")

    # `create_job` with URL to JSON file
    job = connection.create_job("https://jsonbin.example/my/process-graph.json")

