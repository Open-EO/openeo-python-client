..  FYI this file is intended to be inlined (with "include" RST directive)
    in the ProcessBuilder class doc block,
    which in turn is covered with autodoc/automodule from api-processes.rst.


The :py:class:`ProcessBuilder <openeo.processes.ProcessBuilder>` class
is a helper class that implements
(much like the :ref:`openEO process functions <openeo_processes_functions>`)
each openEO process as a method.
On top of that it also adds syntactic sugar to support Python operators as well
(e.g. ``+`` is translated to the ``add`` process).

.. attention::
    As normal user, you should never create a
    :py:class:`ProcessBuilder <openeo.processes.ProcessBuilder>` instance
    directly.

    You should only interact with this class inside a callback
    function/lambda while building a child callback process graph
    as discussed at :ref:`child_callback_callable`.


For example, let's start from this simple usage snippet
where we want to reduce the temporal dimension
by taking the temporal mean of each timeseries:

.. code-block:: python

    def my_reducer(data):
        return data.mean()

    cube.reduce_dimension(reducer=my_reducer, dimension="t")

Note that this ``my_reducer`` function has a ``data`` argument,
which conceptually corresponds to an array of pixel values
(along the temporal dimension).
However, it's important to understand that the ``my_reducer`` function
is actually *not evaluated when you execute your process graph*
on an openEO back-end, e.g. as a batch jobs.
Instead, ``my_reducer`` is evaluated
*while building your process graph client-side*
(at the time you execute that ``cube.reduce_dimension()`` statement to be precise).
This means that that ``data`` argument is actually not a concrete array of EO data,
but some kind of *virtual placeholder*,
a :py:class:`ProcessBuilder <openeo.processes.ProcessBuilder>` instance,
that keeps track of the operations you intend to do on the EO data.

To make that more concrete, it helps to add type hints
which will make it easier to discover what you can do with the argument
(depending on which editor or IDE you are using):

.. code-block:: python

    from openeo.processes import ProcessBuilder

    def my_reducer(data: ProcessBuilder) -> ProcessBuilder:
        return data.mean()

    cube.reduce_dimension(reducer=my_reducer, dimension="t")


Because :py:class:`ProcessBuilder <openeo.processes.ProcessBuilder>` methods
return new :py:class:`ProcessBuilder <openeo.processes.ProcessBuilder>` instances,
and because it support syntactic sugar to use Python operators on it,
and because :ref:`openeo.process functions <openeo_processes_functions>`
also accept and return :py:class:`ProcessBuilder <openeo.processes.ProcessBuilder>` instances,
we can mix methods, functions and operators in the callback function like this:

.. code-block:: python

    from openeo.processes import ProcessBuilder, cos

    def my_reducer(data: ProcessBuilder) -> ProcessBuilder:
        return cos(data.mean()) + 1.23

    cube.reduce_dimension(reducer=my_reducer, dimension="t")

or compactly, using an anonymous lambda expression:

.. code-block:: python

    from openeo.processes import cos

    cube.reduce_dimension(
        reducer=lambda data: cos(data.mean())) + 1.23,
        dimension="t"
    )
