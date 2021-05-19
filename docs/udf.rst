.. _user-defined-functions:

========================================
User-Defined Functions (UDF) explained
========================================

User defined functions are a very important feature of OpenEO. They allow you as a user to
reuse existing code, by submitting it to the backend.

As datacubes can be very large, the backend will only be able to run your code on a smaller chunk
of the whole cube. So you need to help the backend a bit, by designing your code to work on as small
a piece of data as possible.

There are a few different types of operations where UDF's can be used:

1. Applying a process to each pixel: `apply <https://openeo.org/documentation/1.0/processes.html#apply>`_
2. Applying a process to all pixels along a dimension, without changing cardinality: `apply_dimension <https://openeo.org/documentation/1.0/processes.html#apply_dimension>`_
3. Reducing values along a dimension: `reduce_dimension <https://openeo.org/documentation/1.0/processes.html#reduce_dimension>`_
4. Applying a process to all pixels in a multidimensional neighborhood:  `apply_neighborhood <https://openeo.org/documentation/1.0/processes.html#apply_neighborhood>`_

Not all functions will require you to write a custom process. For instance, if you want to take the absolute
value of your datacube, you can simply use the predefined absolute value function. In fact, it is
recommended to try and use predefined functions, as they can be more efficiently implemented.

However, when you have a large piece of code that is hard to transform into predefined openEO functions,
then it makes sense to use the UDF functionality.

The section below gives an example to get you started.


.. note::

    Don not confuse user-defined functions (abbreviated as UDF) with
    **user-defined processes** (sometimes abbreviated as UDP) in openEO,
    which is a way to define and use your own process graphs
    as reusable building blocks.
    see :ref:`user-defined-processes` for more information.


Example: Smoothing timeseries with a user defined function (UDF)
----------------------------------------------------------------


In this example, we start from the ``evi_cube`` that was created in the previous example, and want to
apply a temporal smoothing on it. More specifically, we want to use the "Savitzky Golay" smoother
that is available in the SciPy Python library.


To ensure that openEO understand your function, it needs to follow some rules, the UDF specification.
This is an example that follows those rules:

.. literalinclude:: ../examples/udf/smooth_savitzky_golay.py
    :language: python
    :caption: Example UDF code ``smooth_savitzky_golay.py``
    :name: savgol_udf

The method signature of the UDF is very important, because the backend will use it to detect
the type of UDF.
This particular example accepts a :py:class:`~openeo.rest.datacube.DataCube` object as input and also returns a :py:class:`~openeo.rest.datacube.DataCube` object.
The type annotations and method name are actually used to detect how to invoke the UDF, so make sure they remain unchanged.


Once the UDF is defined in a separate file, we need to load it::

    def load_udf(relative_path):
        with open(relative_path, 'r+') as f:
            return f.read()

    smoothing_udf = load_udf('smooth_savitzky_golay.py')
    print(smoothing_udf)

after that, we can simply apply it along a dimension::

    smoothed_evi = evi_cube_masked.apply_dimension(
        code=smoothing_udf, runtime='Python'
    )


Example: downloading a datacube and executing an UDF locally 
------------------------------------------------------------

Sometimes it is advantageous to run a UDF on the client machine (for example when developing/testing that UDF). 
This is possible by using the convenience function :py:func:`openeo.udf.run_code.execute_local_udf`.
The steps to run a UDF (like the code from ``smooth_savitzky_golay.py`` above) are as follows:

* Run the processes (or process graph) preceding the UDF and download the result in 'NetCDF' or 'JSON' format.
* Run :py:func:`openeo.udf.run_code.execute_local_udf` on the data file.

For example::

    my_process = connection.load_collection(...

    my_process.download('test_input.nc', format='NetCDF')

    smoothing_udf = load_udf('smooth_savitzky_golay.py')

    from openeo.udf import execute_local_udf
    execute_local_udf(smoothing_udf, 'test_input.nc', fmt='netcdf')

Note: this algorithm's primary purpose is to aid client side development of UDFs using small datasets. It is not designed for large jobs.

UDF function names
------------------

There's a predefined set of function signatures that you have to use to implement a UDF:

.. automodule:: openeo.udf.udf_signatures
 :members:



Profile a process server-side
-----------------------------

.. warning::
    Experimental feature - This feature only works on backends running the Geotrellis implementation, and has not yet been
    adopted in the openEO API.

Sometimes users want to 'profile' their UDF on the backend. While it's recommended to first profile it offline, in the
same manner as you can debug UDF's, backends may support profiling directly.
Note that this will only generate statistics over the python part of the execution, therefore it is only suitable for profiling UDFs.

Usage
_____

Only batch jobs are supported! In order to turn on profiling, set 'profile' to 'true' in job options::

        job_options={'profile':'true'}
        ... # prepare the process
        process.execute_batch('result.tif',job_options=job_options)

When the process has finished, it will also download a file called 'profile_dumps.tar.gz':

-   ``rdd_-1.pstats`` is the profile data of the python driver,
-   the rest are the profiling results of the individual rdd id-s (that can be correlated with the execution using the SPARK UI).

Viewing profiling information
_____________________________

The simplest way is to visualize the results with a graphical visualization tool called kcachegrind.
In order to do that, install `kcachegrind <http://kcachegrind.sourceforge.net/>`_ packages (most linux distributions have it installed by default) and it's python connector `pyprof2calltree <https://pypi.org/project/pyprof2calltree/>`_.
From command line run::

       pyprof2calltree rdd_<INTERESTING_RDD_ID>.pstats.

Another way is to use the builtin pstats functionality from within python::

        import pstats
        p = pstats.Stats('restats')
        p.print_stats()

Example
_______

An example code can be found `here <https://github.com/Open-EO/openeo-python-client/tree/master/examples/profiling_example.py>`_ .
