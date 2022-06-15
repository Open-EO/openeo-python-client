.. index:: User-defined functions
.. index:: UDF

.. _user-defined-functions:

######################################
User-Defined Functions (UDF) explained
######################################


While openEO supports a wide range of pre-defined processes
and allows to build more complex user-defined processes from them,
you sometimes need operations or algorithms that are
not (yet) available or standardized as openEO process.
**User-Defined Functions (UDF)** is an openEO feature
(through the `run_udf <https://processes.openeo.org/#run_udf>`_ process)
that aims to fill that gap by allowing a user to express (a part of)
an **algorithm as a Python/R/... script to be run back-end side**.


Ideally, it allows you to embed existing Python/R/... implementations
in an openEO workflow (with some necessary "glue code").
However, it is recommended to try to do as much pre- or postprocessing
with pre-defined processes
before blindly copy-pasting source code snippets as UDFs.
Pre-defined processes are typically well-optimized by the backend,
while UDFs can come with a performance penalty
and higher development/debug/maintenance costs.


.. warning::

    Don not confuse **user-defined functions** (abbreviated as UDF) with
    **user-defined processes** (sometimes abbreviated as UDP) in openEO,
    which is a way to define and use your own process graphs
    as reusable building blocks.
    See :ref:`user-defined-processes` for more information.



Applicability and Constraints
==============================

.. index:: chunking

openEO is designed to work transparently on large data sets
and your UDF has to follow a couple of guidelines to make that possible.
First of all, as data cubes play a central role in openEO,
your UDF should accept and return correct **data cube structures**,
with proper dimensions, dimension labels, etc.
Moreover, the back-end will typically divide your input data cube
in smaller chunks and process these chunks separately (e.g. on isolated workers).
Consequently, it's important that your **UDF algorithm operates correctly
in such a chunked processing context**.

UDFs as apply/reduce "callbacks"
---------------------------------

UDFs are typically used as "callback" processes for "meta" processes
like ``apply`` or ``reduce_dimension`` (also see :ref:`callbackfunctions`).
These meta-processes make abstraction of a datacube as a whole
and allow the callback to focus on a small slice of data or a single dimension.
Their nature instructs the backend how the data should be processed
and can be chunked:

`apply <https://processes.openeo.org/#apply>`_
    Applies a process on *each pixel separately*.
    The back-end has all freedom to choose chunking
    (e.g. chunk spatially and temporally).
    Dimensions and their labels are fully preserved.
    See :ref:`udf_example_apply`

`apply_dimension <https://processes.openeo.org/#apply_dimension>`_
    Applies a process to all pixels *along a given dimension*
    to produce a new series of values for that dimension.
    The back-end will not split your data on that dimension.
    For example, when working along the time dimension,
    your UDF is guaranteed to receive a full timeseries,
    but the data could be chunked spatially.
    All dimensions and labels are preserved,
    except for the dimension along which ``apply_dimension`` is applied:
    the number of dimension labels is allowed to change.

`reduce_dimension <https://processes.openeo.org/#reduce_dimension>`_
    Applies a process to all pixels *along a given dimension*
    to produce a single value, eliminating that dimension.
    Like with ``apply_dimension``, the back-end will
    not split your data on that dimension.
    The dimension along which ``apply_dimension`` is applied must be removed
    from the output.
    For example, when applying ``reduce_dimension`` on a spatiotemporal cube
    along the time dimension,
    the UDF is guaranteed to receive full timeseries
    (but the data could be chunked spatially)
    and the output cube should only be a spatial cube, without a temporal dimension

`apply_neighborhood <https://processes.openeo.org/#apply_neighborhood>`_
    Applies a process to a neighborhood of pixels
    in a sliding-window fashion with (optional) overlap.
    Data chunking in this case is explicitly controlled by the user.
    Dimensions and number of labels are fully preserved.



UDF function names and signatures
==================================

The UDF code you pass to the back-end is basically a Python script
that contains one or more functions.
Exactly one of these functions should have a proper UDF signature,
as defined in the :py:mod:`openeo.udf.udf_signatures` module,
so that the back-end knows what the *entrypoint* function is
of your UDF implementation.


Module ``openeo.udf.udf_signatures``
-------------------------------------


.. automodule:: openeo.udf.udf_signatures
 :members:


Examples
=========

In most of the examples we will, unless noted otherwise,
start from an initial Sentinel2 data cube,
with a couple of bands in a small spatio-temporal extent:

.. code-block:: python

    s2_cube = connection.load_collection(
        "SENTINEL2_L2A",
        spatial_extent={"west": 4.00, "south": 51.04, "east": 4.10, "north": 51.1},
        temporal_extent=["2022-03-01", "2022-03-31"],
        bands=["B02", "B03", "B04"]
    )


.. _udf_example_apply:

Example: ``apply`` with an UDF to rescale pixel values
--------------------------------------------------------

The raw values in the initial ``s2_cube`` data cube are digital numbers
and to get physical reflectance values we have to rescale them.
This is a simple local transformation, without any interaction between pixels,
which is the modus operandi of the ``apply`` processes.

.. note::

    In practice it will be a lot easier and more efficient to do this kind of rescaling
    with pre-defined openEO math processes, for example: ``s2_cube.apply(lambda x: 0.0001 * x)``.
    This is just a very simple illustration to get started with UDFs.

The UDF code is this short script:

.. code-block:: python
    :linenos:
    :caption: ``udf-code.py``

    from openeo.udf import XarrayDataCube

    def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:
        array = cube.get_array()
        array.values = 0.0001 * array.values
        return cube

Some details about this UDF script:

- line 1: We import :py:class:`~openeo.udf.xarraydatacube.XarrayDataCube` to use as type annotation of the UDF function.
- line 3: We define a function named ``apply_datacube``,
  which receives and returns a :py:class:`~openeo.udf.xarraydatacube.XarrayDataCube` instance.
  We follow here the :py:meth:`~openeo.udf.udf_signatures.apply_datacube()` UDF function signature.
- line 4: ``cube`` (a :py:class:`~openeo.udf.xarraydatacube.XarrayDataCube` object) is a thin wrapper
  around the data of the chunk we are currently processing.
  We use :py:meth:`~openeo.udf.xarraydatacube.XarrayDataCube.get_array()` to get this data,
  which is an ``xarray.DataArray`` object.
- line 5: Because our scaling operation is so simple, we can transform the ``xarray.DataArray`` values in-place.
- line 6: Consequently, because the values were updated in-place,
  we don't have to build a new :py:class:`~openeo.udf.xarraydatacube.XarrayDataCube` object
  and can just return the (in-place updated) ``cube`` object again.

We can now call :py:meth:`DataCube.apply() <openeo.rest.datacube.DataCube.apply>`
using the UDF code as follows:

.. code-block:: python
    :linenos:
    :caption: UDF usage example snippet

    from pathlib import Path
    from openeo import UDF

    # Load UDF code from file
    udf_code = Path("udf-code.py").read_text()

    # Create UDF helper object encapsulating the UDF code.
    process = UDF(code=udf_code, runtime="Python", data={"from_parameter": "x"})

    # Pass UDF object as child process to `apply`.
    rescaled = s2_cube.apply(process=process)

    rescaled.download("apply-udf-scaling.nc")

Discussion:

- Line 1 and 5: We use ``pathlib.Path.read_text()`` as a compact way to read
  the UDF code from the ``udf-code.py`` file as a string.
  There are a lot of other ways to achieve the same.
  For example, it is not uncommon to just embed the UDF script as a triple-quoted string
  in your process graph building script.
- Line 8: ``UDF`` is a helper class to build a ``run_udf`` node,
  to be used as child process in the ``apply`` process.
- Line 11: we pass this ``UDF`` object in the ``process`` argument
  to :py:meth:`DataCube.apply() <openeo.rest.datacube.DataCube.apply>`


If we now inspect the band values of the downloaded result,
we see that they fall mainly in a range from 0 to 1 (in most cases even below 0.2),
instead of the original digital number range (thousands):

.. image:: _static/images/udf/apply-rescaled-histogram.png



Example: Smoothing timeseries with a user defined function (UDF)
------------------------------------------------------------------


In this example, we start from the ``evi_cube`` that was created in the previous example, and want to
apply a temporal smoothing on it. More specifically, we want to use the "Savitzky Golay" smoother
that is available in the SciPy Python library.


To ensure that openEO understand your function, it needs to follow some rules, the UDF specification.
This is an example that follows those rules:

.. literalinclude:: ../examples/udf/smooth_savitzky_golay.py
    :language: python
    :caption: Example UDF code ``smooth_savitzky_golay.py``
    :name: savgol_udf

The method signature of the UDF is very important, because the back-end will use it to detect
the type of UDF.
This particular example accepts a :py:class:`~openeo.rest.datacube.DataCube` object as input and also returns a :py:class:`~openeo.rest.datacube.DataCube` object.
The type annotations and method name are actually used to detect how to invoke the UDF, so make sure they remain unchanged.


Once the UDF is defined in a separate file, we need to load it:

.. code-block:: python

    from pathlib import Path

    smoothing_udf = Path('smooth_savitzky_golay.py').read_text()
    print(smoothing_udf)

after that, we can simply apply it along a dimension:

.. code-block:: python

    smoothed_evi = evi_cube_masked.apply_dimension(
        code=smoothing_udf, runtime='Python', dimension="t",
    )


Downloading a datacube and executing an UDF locally
=============================================================

Sometimes it is advantageous to run a UDF on the client machine (for example when developing/testing that UDF). 
This is possible by using the convenience function :py:func:`openeo.udf.run_code.execute_local_udf`.
The steps to run a UDF (like the code from ``smooth_savitzky_golay.py`` above) are as follows:

* Run the processes (or process graph) preceding the UDF and download the result in 'NetCDF' or 'JSON' format.
* Run :py:func:`openeo.udf.run_code.execute_local_udf` on the data file.

For example::

    from pathlib import Path
    from openeo.udf import execute_local_udf

    my_process = connection.load_collection(...

    my_process.download('test_input.nc', format='NetCDF')

    smoothing_udf = Path('smooth_savitzky_golay.py').read_text()
    execute_local_udf(smoothing_udf, 'test_input.nc', fmt='netcdf')

Note: this algorithm's primary purpose is to aid client side development of UDFs using small datasets. It is not designed for large jobs.


Profile a process server-side
==============================


.. warning::
    Experimental feature - This feature only works on back-ends running the Geotrellis implementation, and has not yet been
    adopted in the openEO API.

Sometimes users want to 'profile' their UDF on the back-end. While it's recommended to first profile it offline, in the
same manner as you can debug UDF's, back-ends may support profiling directly.
Note that this will only generate statistics over the python part of the execution, therefore it is only suitable for profiling UDFs.

Usage
------

Only batch jobs are supported! In order to turn on profiling, set 'profile' to 'true' in job options::

        job_options={'profile':'true'}
        ... # prepare the process
        process.execute_batch('result.tif',job_options=job_options)

When the process has finished, it will also download a file called 'profile_dumps.tar.gz':

-   ``rdd_-1.pstats`` is the profile data of the python driver,
-   the rest are the profiling results of the individual rdd id-s (that can be correlated with the execution using the SPARK UI).

Viewing profiling information
------------------------------

The simplest way is to visualize the results with a graphical visualization tool called kcachegrind.
In order to do that, install `kcachegrind <http://kcachegrind.sourceforge.net/>`_ packages (most linux distributions have it installed by default) and it's python connector `pyprof2calltree <https://pypi.org/project/pyprof2calltree/>`_.
From command line run::

       pyprof2calltree rdd_<INTERESTING_RDD_ID>.pstats.

Another way is to use the builtin pstats functionality from within python::

        import pstats
        p = pstats.Stats('restats')
        p.print_stats()

Example
-------


An example code can be found `here <https://github.com/Open-EO/openeo-python-client/tree/master/examples/profiling_example.py>`_ .
