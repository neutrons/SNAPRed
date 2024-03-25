FocusGroupMetric Class Documentation
====================================

FocusGroupMetric is a Pydantic-based class that establishes a direct link between a particular FocusGroup
and an array of CalibrationMetric instances. This class is designed to facilitate the organized association
of focus groups with their respective calibration metrics.


Attributes:
-----------

- focusGroupName (str): Serves as the identifier for the focus group. This name is crucial for distinguishing
  among various focus groups within a dataset or calibration process, ensuring that the metrics are accurately
  associated with the correct group.

- calibrationMetric (List[CalibrationMetric]): Contains a list of CalibrationMetric objects, each representing
  a set of metrics related to the calibration quality of the associated focus group. This attribute effectively
  compiles detailed calibration metrics for a focus group, providing a comprehensive overview of its calibration
  performance and quality.
