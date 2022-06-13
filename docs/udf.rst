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



UDF function names
===================

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
