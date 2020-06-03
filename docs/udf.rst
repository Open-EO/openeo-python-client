========================================
User Defined Functions (UDF's) explained
========================================

User defined functions are a very important feature of OpenEO. They allow you as a user to
reuse existing code, by submitting it to the backend.

As datacubes can be very large, the backend will only be able to run your code on a smaller chunk
of the whole cube. So you need to help the backend a bit, by designing your code to work on as small
a piece of data as possible.

There are a few different types of operations where UDF's can be used:

1. Applying a process to each pixel: https://open-eo.github.io/openeo-api/processreference/#apply
2. Applying a process to all pixels along a dimension, without changing cardinality: apply_dimension
3. Reducing values along a dimension: https://open-eo.github.io/openeo-api/processreference/#reduce

Not all functions will require you to write a custom process. For instance, if you want to take the absolute
value of your datacube, you can simply use the predefined absolute value function. In fact, it is
recommended to try and use predefined functions, as they can be more efficiëntly implemented.

However, when you have a large piece of code that is hard to transform into predefined openEO functions,
then it makes sense to use the UDF functionality.

The section below gives an example to get you started.

Example: Smoothing timeseries with a user defined function (UDF)
----------------------------------------------------------------


In this example, we start from the 'evi_cube' that was created in the previous example, and want to
apply a temporal smoothing on it. More specifically, we want to use the "Savitzky Golay" smoother
that is available in the SciPy Python library.


To ensure that openEO understand your function, it needs to follow some rules, the UDF specification.
This is an example that follows those rules:

.. literalinclude:: ../examples/udf/smooth_savitzky_golay.py
    :caption: UDF code
    :name: savgol_udf

The method signature of the UDF is very important, because the backend will use it to detect
the type of UDF. This particular example accepts a 'DataCube' object as input and also returns a 'DataCube' object.
The type annotations and method name are actually used to detect how to invoke the UDF, so make sure they remain unchanged.

The API of the 'DataCube' class can be found here :ref:`datacube-api`.


Once the UDF is defined in a separate file, we need to load it::

    >>> def get_resource(relative_path):
            return str(Path( relative_path))

        def load_udf(relative_path):
            import json
            with open(get_resource(relative_path), 'r+') as f:
                return f.read()

        smoothing_udf = load_udf('udf/smooth_savitzky_golay.py')
        print(smoothing_udf)

after that, we can simply apply it along a dimension::

    >>> smoothed_evi = evi_cube_masked.apply_dimension(smoothing_udf,runtime='Python')


UDF function names
------------------

There's a predefined set of function signatures that you have to use to implement a UDF:

.. automodule:: openeo_udf.api.udf_signatures
 :members: