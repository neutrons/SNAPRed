Getting Started
===============

.. _getting_started:


Build
-----
Conda
`````
Create your conda envirionment:
::
    conda env create --file environment.yml
    activate SNAPRed


Update if its been a while:
::
    activate SNAPRed
    conda env update --file environment.yml  --prune



Run
---

.. code-block::

    cd src
    python -m snapred



Test
----

.. code-block::

    pytest
