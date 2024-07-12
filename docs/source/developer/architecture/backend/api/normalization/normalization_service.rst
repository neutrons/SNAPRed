NormalizationService Class Documentation
========================================

The Normalization Service orchestrates the normalization of scientific data, including tasks such as calibration, focusing, and smoothing. It employs
various Data Components, including Normalization and NormalizationRecord, to manage normalization specifics. Additionally,
it integrates specialized Services like CalibrationService and DataExportService for calibration and data persistence, aiming to enhance the
normalization workflow's efficiency and accuracy.

Interaction with this service is facilitated through specific requests like NormalizationRequest or SmoothDataExcludingPeaksRequest, each
containing the necessary parameters for the targeted normalization task. This ensures a comprehensive approach to data processing. The
GroceryListBuilder, used via groceryClerk within the service, aids in assembling necessary data items for normalization, streamlining data
preparation and retrieval.


Normalization Workflow:
-----------------------

The service manages the entire normalization process, starting with vanadium data correction, focusing based on groupings, and finishing with data
smoothing to minimize noise. It maintains consistent instrument states across runs and evaluates normalization results for quality assurance.


Data Persistence and Indexing:
------------------------------

Following task completion and validation, the service secures data and metadata using NormalizationExportRequest for storage. It also oversees the
indexing of normalization records to facilitate efficient access and enhance data integrity and reproducibility.


Key Operations:
---------------

- Directs the full normalization process, encompassing data correction, focusing, and
  smoothing.

- Ensures instrument state consistency across runs.

- Reviews normalization results to support data quality assessment.

- Secures normalization data and metadata post-validation.

- Manages normalization record indexing for swift retrieval.

The NormalizationService plays a crucial role in the SNAPRed ecosystem by executing normalization processes with high precision, significantly aiding
the integrity and reproducibility of scientific data analysis.


Attributes:
-----------

- dataFactoryService (DataFactoryService): Manages creation and retrieval of data objects for
  normalization tasks.

- dataExportService (DataExportService): Enables the export of processed data to persistent
  storage systems.

- groceryService (GroceryService): Interfaces with the data layer to fetch and manage
  normalization-relevant data.

- groceryClerk (GroceryListItem.builder): Utilizes the builder pattern to assemble required
  data items for normalization processes.

- diffractionCalibrationService (CalibrationService): Specializes in calibration tasks for
  diffraction data, ensuring precision and accuracy.

- sousChef (SousChef): Prepares the necessary ingredients for the normalization recipe,
  optimizing the preparation phase.
