# Minimal makefile for Sphinx documentation
#

ifeq ($(OS),Windows_NT)
# It should call the python in your virtualenv.
# On Windows the name of the python executable inside the virtualenv is
# generally "python.exe" not python3.exe.
#
# And because you generally want to use pyenv to manage multiple versions of
# Python, you may find that for python3 it actually finds the python3 shim in
# pyenv. And that shill will not work, because our sphinx is not installed there.
PYTHON_EXE = python
else
PYTHON_EXE = python3
endif

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = $(PYTHON_EXE) -m sphinx
SPHINXPROJ    = openeo
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)