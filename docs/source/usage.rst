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
