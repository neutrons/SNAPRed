Getting Started
===============

.. _getting_started:


Instructions for developers
---------------------------

Conda configuration
```````````````````
Create your conda environment using the ``environment.yml`` used by the build servers.

.. code-block:: sh
   :linenos:

    conda env create --file environment.yml
    activate SNAPRed
    python -m pip install -e .

Line 3 installs the code in `editable mode <https://pip.pypa.io/en/stable/cli/pip_install/#cmdoption-e>`_ so other methods can work.

If it has been a while, one can update using

.. code-block:: sh

    activate SNAPRed
    conda env update --file environment.yml  --prune

This can be simplified greatly by using `direnv <https://direnv.net/>`_ and an ``.envrc`` file with contents similar to

.. code-block::

   layout anaconda SNAPRed /opt/anaconda/bin/conda

The location of conda should point at where your version is actually installed.

Starting the gui
----------------

The gui can be started either by using the defined entrypoint

.. code-block:: sh

    python -m snapred

or by executing the entry in the module (which is what the entrypoint is defined as)

.. code-block:: sh

    python -m snapred



Other common operations
-----------------------

The test framework can be run using

.. code-block:: sh

   python -m pytest

Note that the tests currently have a fixture that tests for the presence of a directory containing "large" test files.

The documentation can be run using

.. code-block:: sh

   cd docs
   make html

`Sphinx <https://www.sphinx-doc.org/en/master/>`_ has been configured to turn warnings into errors to make it more clear that there are issues with the documentation.
