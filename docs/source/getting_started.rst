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
