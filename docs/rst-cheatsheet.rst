
reStructuredText is a plain text markup syntax primarily used for Python documentation.
While it is loosing some ground to Markdown, it is still widely used in Python community.

This is non-exhaustive cheatsheet listing some conventions and common constructs
used in the openEO Python Client Library documentation.


============
Page Heading
============

Heading
=======

Subheading
----------

Sub-subheading
``````````````


*emphasis (italics)* and **strong emphasis (bold)**

``literal inline code``


.. warning::

    be warned!

.. note::

    just a note

Generic code block
(note the double colon at the end here)::

    indented code block here


More explicit code block with language hint (and no need for double colon)

.. code-block:: python

    print("python code here")

.. code-block:: pycon

    >>> 3 + 5
    8



References:

- define a reference target (e.g. just before a header/title) with::

    .. _target:

- refer to the reference with::

    :ref:`target`
