CalibrationExportRequest Class Documentation
============================================

CalibrationExportRequest is a Pydantic model specifically designed to streamline the process
of exporting and saving completed calibrations that have undergone satisfactory assessments.
This class plays a crucial role in the calibration workflow, serving as a structured method
for conveying the outcomes of calibration assessments back to the system for storage and future
reference. By encapsulating both the detailed calibration process and parameters alongside the
indexing information, CalibrationExportRequest ensures a seamless transition from calibration
assessment to permanent storage.


Attributes:
-----------

- createRecordRequest (CreateCalibrationRecordRequest): Contains detailed information about the calibration process,
  including the calibration parameters, run number, crystallographic information, and related workspace
  data. This comprehensive record is essential for documenting the calibration's specifics, facilitating
  future analyses or replication of the calibration.

- createIndexEntryRequest (CreateIndexEntryRequest): Provides indexing information for the calibration,
  including run number, version, comments, and additional metadata that aids in organizing and
  retrieving calibration data within the system. The index entry ensures that the calibration can
  be efficiently referenced and accessed when needed.
