NormalizationRequest Class Documentation
===================================================

NormalizationExportRequest.py introduces the NormalizationExportRequest class, designed for the packaging and saving of detailed information
regarding completed normalization processes. Aimed at encapsulating the results of normalization processes, including process details and contextual
metadata, this class facilitates disk storage post-user approval. It incorporates references to NormalizationRecord for detailing the process
specifics and NormalizationIndexEntry for indexing metadata, ensuring the preservation of critical data in an organized and accessible manner. This
approach supports data integrity, adherence to research standards, and the enhancement of reproducibility across scientific endeavors.


Attributes:
-----------

- normalizationRecord (NormalizationRecord): Contains detailed information about the
  normalization process, including run numbers, calibration details, and parameters
  used for the normalization. This component encapsulates the entirety of the
  normalization procedure's output, providing a comprehensive record of the operation.

- normalizationIndexEntry (NormalizationIndexEntry): Stores metadata relevant for indexing
  and later retrieval of the normalization record. This includes identifiers,
  versioning information, and potentially comments or tags for easier search and
  categorization. This element is essential for organizing and accessing the
  normalization records within a database or file system.
