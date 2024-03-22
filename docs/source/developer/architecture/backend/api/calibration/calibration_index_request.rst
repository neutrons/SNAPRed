CalibrationIndexRequest Class Documentation
===========================================

CalibrationIndexRequest is a Pydantic model crafted to streamline the process of querying
calibration records based on the instrument state associated with a specific run. This
class encapsulates the essential parameters required to effectively filter and retrieve
calibration data that align with the conditions and settings of a particular experimental
run.


Attributes:
-----------

- run (RunConfig): Captures the configuration details of the run in question, including
  instrument settings, run number, and any other pertinent parameters defined within the
  RunConfig model. This configuration is key to identifying which calibration records are
  relevant to the experimental conditions of the run, ensuring that queries return the most
  appropriate and applicable data.
