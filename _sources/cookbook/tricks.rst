===============================
Miscellaneous tips and tricks
===============================


.. _process_graph_export:

Export a process graph
-----------------------

You can export the underlying process graph of
a :py:class:`~openeo.rest.datacube.DataCube`, :py:class:`~openeo.rest.vectorcube.VectorCube`, etc,
to a standardized JSON format, which allows interoperability with other openEO tools.

For example, use :py:meth:`~openeo.rest.datacube.DataCube.print_json()` to directly print the JSON representation
in your interactive Jupyter or Python session:

.. code-block:: pycon

    >>> dump = cube.print_json()
    {
      "process_graph": {
        "loadcollection1": {
          "process_id": "load_collection",
    ...

Or save it to a file, by getting the JSON representation first as a string
with :py:meth:`~openeo.rest.datacube.DataCube.to_json()`:

.. code-block:: python

    # Export as JSON string
    dump = cube.to_json()

    # Write to file in `pathlib` style
    export_path = pathlib.Path("path/to/export.json")
    export_path.write_text(dump, encoding="utf8")

    # Write to file in `open()` style
    with open("path/to/export.json", encoding="utf8") as f:
        f.write(dump)


.. warning::

    Avoid using methods like :py:meth:`~openeo.rest.datacube.DataCube.flat_graph()`,
    which are mainly intended for internal use.
    Not only are these methods subject to change, they also lead to representations
    with interoperability and reuse issues.
    For example, naively printing or automatic (``repr``) rendering of
    :py:meth:`~openeo.rest.datacube.DataCube.flat_graph()` output will roughly look like JSON,
    but is in fact invalid: it uses single quotes (instead of double quotes)
    and booleans values are title-case (instead of lower case).




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
    connection.execute(
        """
        {
            "add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": true}
        }
    """
    )

    # `download` with local path to JSON file
    connection.download("path/to/my-process-graph.json")

    # `create_job` with URL to JSON file
    job = connection.create_job("https://jsonbin.example/my/process-graph.json")


.. _legacy_read_vector:


Legacy ``read_vector`` usage
----------------------------

In versions up to 0.35.0 of the openEO Python client library,
there was an old, deprecated feature in geometry handling
of :py:class:`~openeo.rest.datacube.DataCube` methods like
:py:meth:`~openeo.rest.datacube.DataCube.aggregate_spatial()` and
:py:meth:`~openeo.rest.datacube.DataCube.mask_polygon()`
where you could pass a *backend-side* path as ``geometries``, e.g.:

.. code-block:: python

    cube = cube.aggregate_spatial(
        geometries="/backend/path/to/geometries.json",
        reducer="mean",
    )

The client would handle this by automatically adding a ``read_vector`` process
in the process graph, with that path as argument, to instruct the backend to load the geometries from there.
This ``read_vector`` process was however a backend-specific, experimental and now deprecated process.
Moreover, it assumes that the user has access to (or at least knowledge of) the backend's file system,
which violates the openEO principle of abstracting away backend-specific details.

In version 0.36.0, this old deprecated ``read_vector`` feature has been *removed*,
to allow other and better convenience functionality
when providing a string in the ``geometries`` argument:
e.g. load from a URL with standard process ``load_url``,
or load GeoJSON from a local clientside path.

If your workflow however depends on the old, deprecated ``read_vector`` functionality,
it is possible to reconstruct that by manually adding a ``read_vector`` process in your workflow,
for example as follows:

.. code-block:: python

    from openeo.processes import process

    cube = cube.aggregate_spatial(
        geometries=process("read_vector", filename="/backend/path/to/geometries.json"),
        reducer="mean",
    )

Note that this is also works with older versions of the openEO Python client library.
