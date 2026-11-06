
Best practices, coding style and general tips
===============================================

This is a collection of guidelines regarding best practices,
coding style and usage patterns for the openEO Python Client Library.

It is in the first place an internal recommendation for openEO *developers*
to give documentation, code examples, demo's and tutorials
a *consistent* look and feel,
following common software engineering best practices.
Secondly, the wider audience of openEO *users* is also invited to pick up
a couple of tips and principles to improve their own code and scripts.


Background and inspiration
---------------------------

While some people consider coding style a personal choice or even irrelevant,
there are various reasons to settle on certain conventions.
Just the fact alone of following conventions
lowers the bar to get faster to the important details in someone else's code.
Apart from taste, there are also technical reasons to pick certain rules
to *streamline the programming workflow*,
not only for humans,
but also supporting tools (e.g. minimize risk on merge conflicts).

While the Python language already has a strong focus on readability by design,
the Python community is strongly gravitating to even more strict conventions:

- `pep8 <https://peps.python.org/pep-0008/>`_: the mother of all Python code style guides
- `black <https://black.readthedocs.io/en/stable/>`_: an opinionated code formatting tool
  that gets more and more traction in popular, high profile projects.

This openEO oriented style guide will highlight
and build on these recommendations.


General code style recommendations
------------------------------------

- Indentation with 4 spaces.
- Avoid star imports (``from module import *``).
  While this seems like a quick way to import a bunch of functions/classes,
  it makes it very hard for the reader to figure out where things come from.
  It can also lead to strange bugs and behavior because it silently overwrites
  references you previously imported.


Line (length) management
--------------------------

While desktop monitors offer plenty of (horizontal) space nowadays,
it is still a common recommendation to *avoid long source code lines*.
Not only are long lines hard to read and understand,
one should also consider that source code might still be viewed
on a small screen or tight viewport,
where scrolling horizontally is annoying or even impossible.
Unnecessarily long lines are also notorious
for not playing well with version control tools and workflows.

Here are some guidelines on how to split long statements over multiple lines.

Split long function/method calls directly after the opening parenthesis
and list arguments with a standard 4 space indentation
(not after the first argument with some ad-hoc indentation).
Put the closing parenthesis on its own line.

.. code-block:: python

    # Avoid this:
    s2_fapar = connection.load_collection("TERRASCOPE_S2_FAPAR_V2",
                                          spatial_extent={'west': 16.138916, 'east': 16.524124, 'south': 48.1386, 'north': 48.320647},
                                          temporal_extent=["2020-05-01", "2020-05-20"])

    # This is better:
    s2_fapar = connection.load_collection(
        "TERRASCOPE_S2_FAPAR_V2",
        spatial_extent={"west": 16.138916, "east": 16.524124, "south": 48.1386, "north": 48.320647},
        temporal_extent=["2020-05-01", "2020-05-20"],
    )

.. TODO how to handle chained method calls



Jupyter(lab)  tips and tricks
-------------------------------

-   Add a cell with ``openeo.client_version()`` (e.g. just after importing all your libraries)
    to keep track of which version of the openeo Python client library you used in your notebook.

.. TODO how to work with "helper" modules?
