Getting Started
===============

.. _getting_started:

Developer installation
----------------------

.. _setup-dev-env:

Pixi configuration
^^^^^^^^^^^^^^^^^^

Create your pixi environment using the ``pyproject.toml`` used by the build servers.
From the root of the source, type the following::

    pixi install

To update your environment with changes from the ``pyproject.toml``, type::

    pixi install

.. note::
    For more information on the available tasks, type ``pixi run --list``.
.. note::
    For more details, please see the `pixi documentation <https://pixi.ws/latest/>`_.
.. note::
    To install ``pixi``, please see the `installation guide <https://pixi.ws/latest/getting_started/>`_.

IDE configuration (optional)
----------------------------
The following is for configuring VSCode as the IDE
but other IDEs may have similar configuration options.

VSCode can be configured to use the ``pixi`` environment by adding the following
to the project's ``.vscode/settings.json`` file:

.. code-block:: json

    {
        "python.defaultInterpreterPath": ".pixi/envs/default/bin/python"
    }

Starting the gui
----------------

The gui can be started either by using the defined entrypoint

.. code-block:: sh

    pixi run snapred

or by executing the entry in the module (which is what the entrypoint is defined as)

.. code-block:: sh

    pixi run python -m snapred



Other common operations
-----------------------

Running tests
^^^^^^^^^^^^^

The unit test framework can be run using

.. code-block:: sh

   pixi run test

Note that the tests currently have a fixture that tests for the presence of a directory containing "large" test files.

Integration tests
^^^^^^^^^^^^^^^^^

Integration tests require additional setup and test data. These tests are more comprehensive and test the full workflow of the application.

To run integration tests locally:

.. code-block:: sh

   # Run integration tests with the integration test environment
   env=tests/resources/integration_test pixi run pytest -m integration

   # Run with verbose output to see detailed test progress
   env=tests/resources/integration_test pixi run pytest -m integration -v

   # Run with xvfb for headless GUI testing (Linux only)
   xvfb-run --server-args="-screen 0 1280x1024x16" -a env=tests/resources/integration_test pixi run pytest -m integration

   # Run specific integration test files
   env=tests/resources/integration_test pixi run pytest tests/integration/test_diffcal.py -m integration

.. note::
    Integration tests may take significantly longer to run than unit tests as they test complete workflows.

.. note::
    Some integration tests require specific test data files and may be skipped if the data is not available.

.. warning::
    Integration tests may create temporary files and workspaces. Ensure you have adequate disk space and permissions.

GUI tests
^^^^^^^^^

GUI tests can be run to test the graphical user interface:

.. code-block:: sh

   # Run GUI tests with headless display (Linux)
   env=tests/resources/headcheck.yml xvfb-run --server-args="-screen 0 1280x1024x16" --auto-servernum pixi run snapred --headcheck

   # On systems with a display, you can run without xvfb
   env=tests/resources/headcheck.yml pixi run snapred --headcheck

Building documentation
^^^^^^^^^^^^^^^^^^^^^^^

The documentation can be built using

.. code-block:: sh

   pixi run --environment docs build-docs

For development, you can use auto-rebuilding documentation that updates on file changes:

.. code-block:: sh

   pixi run --environment docs docs-autobuild

Then visit http://localhost:8000 to view the documentation.

You can also serve the built documentation locally:

.. code-block:: sh

   pixi run --environment docs docs-serve

`Sphinx <https://www.sphinx-doc.org/en/master/>`_ has been configured to turn warnings into errors to make it more clear that there are issues with the documentation.

Cleaning build artifacts
^^^^^^^^^^^^^^^^^^^^^^^^

To clean all build artifacts:

.. code-block:: sh

   pixi run clean-all

Or clean specific artifacts:

.. code-block:: sh

   # Clean documentation build artifacts
   pixi run clean-docs

   # Clean PyPI build artifacts
   pixi run clean-pypi

   # Clean conda build artifacts
   pixi run clean-conda
