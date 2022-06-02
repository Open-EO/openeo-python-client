

This is a custom documentation build to support SRR related reporting.

Build instructions:

- install necessary dependencies
  - openeo package and Sphinx (dev dependency): e.g. from project root:

        pip install -e .[dev]

  - LaTeX, e.g. see [Sphinx docs on LaTeX builder requirements](https://www.sphinx-doc.org/en/master/usage/builders/index.html#sphinx.builders.latex.LaTeXBuilder)
- Build documention with Sphinx, e.g. from `reporting` dir:

      make clean html latexpdf

