Resources
=========

Summary
-------

This package contains various forms of static configuration SNAPRed
depends on. This may include filepaths, workspace name templates,
default ui parameters, ect.

The ``application.yml`` file will probably be the most relevant and
frequently changed.

.. _applicationyml:

Application.yml
---------------

This is a static configuration file that stores data categorically in
the context of `SNAP <SNAP>`__. It follows basic yml syntax and supports
key substitution.

Key Substitution
~~~~~~~~~~~~~~~~

.. code:: yaml

   instrument:
     name: SNAP
     home: /SNS/SNAP/
     calibration:
       home: ${instrument.home}shared/Calibration

In this example, the value for ``instrument.calibration.home`` is know
to be in the same folder as ``instrument.home``, so we can use the key
substitution syntax ``${key}`` to allow Calibration Home to change
dynamically with its parent.

Keys of Note
~~~~~~~~~~~~

+-----------------+---------------------------------------------------+
| Key             | Explanation                                       |
+=================+===================================================+
| environment     | This key will trigger yml's of the same name in   |
|                 | the resource folder to be loaded over the default |
|                 | application.yml                                   |
+-----------------+---------------------------------------------------+
| instrument.home | You will likely want to edit this for local       |
|                 | testing as SNAPRed is dependent on a certain file |
|                 | structure to exist on the target system.          |
+-----------------+---------------------------------------------------+
| logging.level   | This tells SNAPRed what log level to start on by  |
|                 | default. CRITICAL = 50, ERROR = 40, WARNING = 30, |
|                 | INFO = 20, DEBUG = 10                             |
+-----------------+---------------------------------------------------+
| cis_mode        | Currently this preserves intermediate workspaces  |
|                 | so that the CIS can debug results, setting it to  |
|                 | False is typically faster and should be the case  |
|                 | for production. Current default = True            |
+-----------------+---------------------------------------------------+

default
-------

This folder contains a file structure matching the path used by
consumers of the `InterfaceController <InterfaceController>`__ which it
uses to map a terminal ``payload.json`` as the default values for a
given call. The original intention was for these values to map
one-to-one with views used to request backend calls but may be
depreciated for a more tailored implementation.

.. _asciitxt:

ascii.txt
---------

This is simply the splash text art SNAPRed displays in terminal at
startup.

.. _styleqss:

style.qss
---------

This is the file that stores its QT stylesheet. Custom css-like style
overrides go here, and will be applied when SNAPRed is launched
as a stand-alone application.

.. _workbenchstyleqss:

workbench_style.qss
-------------------

This is the file that stores its QT stylesheet. Custom css-like style
overrides go here, and will be applied when SNAPRed is launched
from Mantid workbench.
