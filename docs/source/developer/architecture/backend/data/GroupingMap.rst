.. _grouping-map-design:

================================================
Grouping-schema map (as ``GroupingMap`` object):
================================================

*********
Overview:
*********

Each :term:`detector <Detector>` on the :term:`instrument <Instrument>` is comprised of multiple :term:`pixels <Pixel>`, which are the smallest physical elements used to measure particle flux.  These pixels are identified by their indices (global to the instrument), which are colloquially referred to as :term:`detector-ids <Detector ID>`.

During the first stages of processing data from a specified run, data from groups of pixels are combined and processed together.  A :term:`grouping schema <Pixel Group>` is used to assign pixels to groups, thereby associating each detector-id with a group number.  Such a schema is most commonly represented in Mantid as a grouping workspace.  The specific representation of the grouping workspace is as a map between detector-id and group number, implemented as a vector of consecutive :term:`spectra <Spectrum>`, where each spectrum number is the detector-id, and each single value is the corresponding group number.

Depending on the requirements of data processing, several possible grouping schema may be used. Examples of available grouping schemas are, "all": a single group including all pixels in the instrument; "column": each group includes pixels belonging to a column of detectors; "bank": each group includes pixels belonging to a detector bank.

***************
Implementation:
***************

The available grouping schema have been identified by the :term:`CIS <CIS>`, and are collected together in a default grouping-schema map represented on-disk as ``defaultGroupingMap.json``, located at <:term:`instrument.calibration.powder.grouping.home<Config>`>.  The grouping-schema map provides a mapping between the common name for a grouping schema, and an :term:`XML` or :term:`HDF5` format file which represents the specifics of that schema, which may then be loaded into a grouping workspace.  The grouping-schema map is represented on-disk in :term:`JSON` format, and each schema entry consists of the common-name for the schema, e.g. "column", followed by either a relative or absolute path to the filename for that schema.  In this filename specification any relative path will be relative to <instrument.calibration.powder.grouping.home>, although an absolute path can be used to refer to a grouping schema that isn't located in that directory.  In the JSON file, these lists of available schema are additionally divided into those applicable to :term:`native<Native Mode>` mode, and those applicable to :term:`lite<Lite Mode>` mode.

For a specific :term:`instrument state<Instrument State>`, it is possible that only a subset of the default grouping schemas is applicable, or perhaps there may be additional grouping schemas which are required.  For this reason, during state initialization a state-registered copy of the JSON-file for the default grouping-schema map ``groupingMap.json`` is created at the state root directory.  This file may then be edited as required, to either add or exclude applicable grouping schemas.  In this state-specific JSON-file, the :term:`stateID <State ID>` field is used to register this grouping-schema map to the specific state.

To facilitate editing by hand, the JSON representation of the grouping-schema map consists of two lists of ``FocusGroup`` representations.  One list includes ``FocusGroup`` applicable to the instrument in :term:`native <Native Mode>` mode, and the other list those applicable in :term:`lite <Lite Mode>` mode.

Usage:
======
At any point after a state has been initialized, the state's grouping-schema map may be accessed using:

1. Import the necessary classes:

.. code-block:: python

   from from snapred.backend.dao.state import GroupingMap
   from snapred.backend.data.DataFactoryService import DataFactoryService
   
2. Access the state's grouping map:

.. code-block:: python

   runNumber = "52280"
   dataService = DataFactoryService()
   groupingMap = dataService.getGroupingMap(runNumber)

3. Obtain the filename for the "column" (lite mode) grouping schema:

.. code-block:: python

   groupingScheme = "column"
   useLiteMode = True

   # A relative path here will refer to <instrument.calibration.powder.grouping.home>
   columnFilename = groupingMap.getMap(useLiteMode)[groupingScheme].definition


Initialization specifics:
=========================
  
The template file ``defaultGroupingMap.json`` must be filled-in and placed "by hand" at <instrument.calibration.powder.grouping.home> by the CIS.

The creation of the ``groupingMap.json`` file located at the state-root directory is triggered at two distinct code locations, whenever a state-root directory doesn't exist.  These trigger locations are at: 1) ``LocalDataService.readStateConfig``, and (2) ``LocalDataService.initializeState``.  Once the ``groupingMap.json`` file has been created, as it may contain user-specified information, it will never be overwritten.

The ``GroupingMap`` data object is an attribute of the ``StateConfig`` data object, however it is represented on disk as a separate JSON file.  In many cases, the ``GroupingMap`` is required when the complete ``StateConfig`` is not.  For this reason, it is generally accessed using ``LocalDataService.readGroupingMap`` (see the "defects" section below), rather than ``LocalDataService.readStateConfig``.  This is why two separate initialization paths have been implemented.  Generally, the ``initializeState`` path is used by the application code and unit-test code, whereas the ``readStateConfig`` path at present only seems to be used by the unit-test code. 

Validation specifics:
=====================
  
:term:`Pydantic` validators are used to validate the ``GroupingMap`` instance at point of loading.  Any grouping-schema files which do not exist (or have an incorrect format) are not loaded to the resident ``GroupingMap`` -- in these cases, warnings will be logged but the loading will be allowed to continue.  When the ``GroupingMap`` is loaded using the ``readStateConfig`` path, the validity of the ``GroupingMap.stateId`` is checked to ensure that it corresponds to that of the state itself.

Potential defects:
==================
  
  * Although this is a fine point, the fact that we now have a public method ``LocalDataService.readGroupingMap`` which falls back automatically to load the ``defaultGroupingMap.json`` when the state has not been initialized is a potential defect.  During the initial design phase it was intended that the ``GroupingMap`` would always be accessed only via its parent ``StateConfig`` object.  Since accessing the ``GroupingMap`` by itself is definitely an acceptable goal, perhaps a better implementation would provide the current ``_readGroupingMap`` as the public method, and would produce an error if called on an uninitialized state.  On this topic, it will also be noted that this access requirement possibly indicates that ``GroupingMap`` should *not* really be part of ``Stateconfig`` at all, or that some aspect of ``StateConfig`` needs to be redesigned so that its loading is no longer an issue in these cases (e.g. ``Optional[Calibration]`` is a possibility here).  
  * If the path provided to a grouping schema (e.g. in ``defaultGroupingMap.json``) is a relative path, it is relative to <instrument.calibration.powder.grouping.home>.  This relative-path aspect provides additional information to the user (e.g. that this is a "standard" grouping schema, in some sense).  For this reason, it is a defect (to be fixed shortly) that when the ``groupingMap.json`` is written to disk, the paths have all been converted to an absolute form.
  
  
