Usage
=====

.. _installation:

Installation
------------


Under Construction!

Application settings
--------------------

Settings for the application are shipped with the package in the `application.yml <https://github.com/neutrons/SNAPRed/blob/next/src/snapred/resources/application.yml>`_ file.
There is also a `test application.yml <https://github.com/neutrons/SNAPRed/blob/next/tests/resources/application.yml>`_ file used in tests.
Most of what is included in the settings is a parameterization of options for the instrument.
It may be desireable to override some of the settings which can be done via environment injection.
For example

.. code-block:: yaml
   :caption: Example environment ``myenv.yml``

   environment: myenv

   instrument:
     home: ~/snapred-testing/

Can be used to override the ``instrument.home`` directory to ``~/snapred-testing/``.
At this time, only the ``instrument.home`` and ``samples.home`` directories will expand ``~/`` to a directory.
This file can be supplied via command line injection

.. code-block:: sh

   env=myenv.yml snapred

Using the Test Data Repo
------------------------

When using the data repo for the very first time, be sure to run ``git lfs install`` outside the repo, otherwise the data
will not show up correctly. You should not have to run this again later.
To simply get the data, just run ``git submodule update --init --recursive`` and the data will appear under
``tests/data/snapred-data``. You can now update the environmnet YAML file that you will use for tests and point to the
test data using ``${module.root}/data/snapred-data``, assuming you are just running pytests. If you are just trying to
run SNAPRed normally, you need to change the pathing to be ``${module.root}/../../data/snapred-data`` or use the full
absolute path. If you make any changes to the test repo, be sure to update the git LFS reference and commit it to this repo!

For more info about the test data repo, go to the readme `here. <https://code.ornl.gov/sns-hfir-scse/infrastructure/test-data/snapred-data/-/blob/main/README.md?ref_type=heads>`_
