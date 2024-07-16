NormalizationRecord Class Documentation
=======================================

NormalizationRecord.py defines the NormalizationRecord class, representing normalization records for scientific data normalization operations. It
defines a structured data model capturing key parameters and metadata about a normalization operation without direct inputs, using a Pydantic
BaseModel.

Upon instantiation, NormalizationRecord objects are created, containing attributes like runNumber, backgroundRunNumber, smoothingParameter, and
others, detailing identifiers, parameters, and contextual data essential for describing normalization steps.

The class models normalization information in a structured, validated manner, capturing all relevant metadata and context for data management and
analysis.


Attributes:
-----------

- runNumber (str): Unique identifier for the normalization run. This is essential for
  uniquely identifying and referencing each normalization operation within a dataset
  or system.

- backgroundRunNumber (str): Identifier for the background run associated with this
  normalization. This detail is crucial for understanding the context and conditions
  under which the normalization was performed.

- smoothingParameter (float): Controls the amount of smoothing applied during normalization.
  The smoothing parameter is a critical factor in the normalization process, affecting
  the outcome and quality of the normalized data.

- calibration (Calibration): Calibration data used for this normalization. Calibration is
  key to ensuring that the normalization is accurate and reflects the true state of the
  instrument or system being normalized.

- workspaceNames (List[str]): List of workspace names associated with this normalization,
  defaulting to an empty list. These names facilitate the organization and retrieval of
  workspaces related to the normalization.

- version (Optional[int]): Version of the normalization record; optional, may be None.
  Allows for tracking of different versions of the normalization record as it is
  updated or revised over time.

- dMin (float): Minimum d-spacing value considered in this normalization. The dMin value is
  important for defining the lower limit of the data range to be normalized, ensuring
  that the normalization process is tailored to the specific data set.
