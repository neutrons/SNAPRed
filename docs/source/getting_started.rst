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

Local Mantid development (optional)
-----------------------------------

SNAPRed supports development with a local Mantid build instead of the conda-distributed packages.
This is useful for developers who need to make simultaneous changes to both SNAPRed and Mantid.

Prerequisites
^^^^^^^^^^^^^

- A local Mantid build directory (e.g., from building Mantid from source)
- Access to the Mantid source directory

Note: The Python interface files are automatically accessed from the source directory, eliminating the need to manually copy them to the build directory.

Setup
^^^^^

1. **Configure the build and source paths** in ``pyproject.toml``:

   .. code-block:: toml

       [tool.pixi.feature.local-mantid.activation.env]
       MANTID_BUILD_DIR = "/path/to/your/mantid/build"
       MANTID_BUILD_SRC = "/path/to/your/mantid/source"

   **Examples:**

   - Build: ``/home/username/mantid/build``, Source: ``/home/username/mantid/source``
   - Build: ``/usr/local/mantid/build``, Source: ``/usr/local/mantid/source``
   - Build: ``/opt/mantid/build``, Source: ``/opt/mantid/source``

2. **Install the local-mantid environment**:

   .. code-block:: sh

       pixi install --environment local-mantid

Usage
^^^^^

**Test the local Mantid configuration:**

.. code-block:: sh

    pixi run --environment local-mantid test-local-mantid

**Comprehensive test (including workbench components):**

.. code-block:: sh

    pixi run --environment local-mantid test-local-mantid-full

**Debug environment variables:**

.. code-block:: sh

    pixi run --environment local-mantid debug-local-mantid-env

**Comprehensive test suite (recommended for troubleshooting):**

.. code-block:: sh

    pixi run --environment local-mantid test-local-mantid-comprehensive

**Start SNAPRed with local Mantid:**

.. code-block:: sh

    pixi run --environment local-mantid snapred-local-module

**Enter development shell:**

.. code-block:: sh

    pixi shell --environment local-mantid

How it works
^^^^^^^^^^^^

The ``local-mantid`` environment:

- **Uses the** ``mantid-developer`` **package directly** - This automatically includes all the same build tools, libraries, and dependencies that Mantid developers use, ensuring perfect compatibility and automatic updates when the mantid-developer environment changes
- **Automatically accesses Python interface from source** - The ``MANTID_BUILD_SRC`` variable points directly to the source directory, eliminating the need to manually copy Python interface files
- **Handles package conflicts via environment variables** - While ``mantid-developer`` installs conda versions of ``mantid``, ``mantidworkbench``, and ``mantidqt``, our environment variables ensure your local build takes precedence:

  - ``PYTHONPATH``: Points to local Python interface from source directory first, then build directory
  - ``LD_LIBRARY_PATH``: Points to local shared libraries first
  - ``MANTIDPATH``: Points to local build directory
  - ``MANTID_DATA_PATH``: Points to local data directory
  - ``MANTID_FRAMEWORK_PATH``: Points to local framework libraries

This approach exactly mirrors the typical developer workflow of using the ``mantid-developer`` environment, but automatically stays in sync with any dependency changes made by the Mantid team.

Troubleshooting
^^^^^^^^^^^^^^^

**Import errors:**

- Ensure the source directory path is correct and contains ``Framework/PythonInterface/mantid/``
- Run ``pixi run --environment local-mantid debug-local-mantid-env`` to verify environment variables

**Library errors:**

- Check that ``${MANTID_BUILD_DIR}/lib`` contains the required shared libraries
- Verify the build completed successfully

**Path conflicts (workbench still using site-packages):**

- Run ``pixi run --environment local-mantid test-local-mantid-comprehensive`` for a complete diagnosis
- Run ``pixi run --environment local-mantid test-local-mantid-full`` to verify all components are using local build
- Check that both ``MANTID_BUILD_DIR`` and ``MANTID_BUILD_SRC`` are set correctly
- Ensure the environment variables are taking precedence by checking the output of ``debug-local-mantid-env``

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
