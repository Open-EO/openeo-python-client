
Best practices, coding style and general tips
===============================================

This is a collection of guidelines regarding best practices,
coding style and usage patterns for the openEO Python Client Library.
It is in the first place a recommendation aimed internally at openEO developers
to give code examples, demo's, documentation and tutorials
a **consistent** look and feel and to promote software engineering best practices.
Secondly, the wider audience of openEO users is also invited to pick up
a couple of tips and principles to improve their own code and scripts.


Background and inspiration
---------------------------

While one could argue that coding style is an arbitrary, somewhat personal choice,
there are technical and practical reasons to prefer certain rules.
For example, as version control tools like git get more widespread,
it is beneficial to adopt a coding style that optimizes
the *signal/noise ratio* of code diffs and history,
and minimizes the risk on merge conflicts.
Likewise, while desktop monitors have plenty of space nowadays,
you still avoid long source code lines
because your code might still be viewed in a constrained viewport, widget or device.

For the Python language, which, by design, already has a strong focus on readability,
there is already a strong foundation of code style recommendations like

- `pep8 <https://peps.python.org/pep-0008/>`_: the mother of all Python code style guides
- `black <https://black.readthedocs.io/en/stable/>`_: an opinionated code formatting tool
  that gets more and more traction in high profile projects and packages.

This openEO oriented style guide will highlight recommendations from these sources
and iterate on them in the context of openEO usage patterns.


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

It is a common recommendation to *avoid long source code lines*.
Not only are long lines hard to read and annoying to scroll horizontally,
they also don't play well with version control tools.
Here are some guidelines on how to split long statements over multiple line.

Split long function/method calls directly after the parenthesis
and list arguments with a standard 4 space indentation,
not after the first argument with some ad-hoc indentation.
Put the closing parenthesis on its own line.

.. code-block:: Python

    # Avoid this:
    s2_fapar = connection.load_collection("TERRASCOPE_S2_FAPAR_V2",
                                          spatial_extent={'west': 16.138916, 'east': 16.524124, 'south': 48.1386, 'north': 48.320647},
                                          temporal_extent=["2020-05-01", "2020-05-20"])

    # This is better:
    s2_fapar = connection.load_collection(
        "TERRASCOPE_S2_FAPAR_V2",
        spatial_extent={'west': 16.138916, 'east': 16.524124, 'south': 48.1386, 'north': 48.320647},
        temporal_extent=["2020-05-01", "2020-05-20"],
    )

.. TODO how to handle chained method calls



Jupyter(lab)  tips and tricks
-------------------------------

-   Add a cell with ``openeo.client_version()`` (e.g. just after importing all your libraries)
    to keep track of which version of the openeo Python client library you used in your notebook.

.. TODO how to work with "helper" modules?