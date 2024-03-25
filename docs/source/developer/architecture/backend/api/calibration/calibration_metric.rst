CalibrationMetric Class Documentation
=====================================

CalibrationMetric is a model class developed using Pydantic, aimed at capturing and quantifying the quality
of calibration procedures in relation to established benchmarks and historical calibration data. This class
is structured to include a comprehensive set of metrics that assess both the consistency and strain of
calibrations, providing a multifaceted view of calibration quality.


Attributes:
-----------

- sigmaAverage (float): Represents the average sigma value across a calibration dataset. Sigma values typically
  quantify the variability or consistency of the calibration, offering insight into the precision of the calibration
  process.

- sigmaStandardDeviation (float): Measures the standard deviation of sigma values within the calibration dataset,
  providing a statistical assessment of calibration consistency and variability.

- strainAverage (float): Captures the average strain observed during the calibration process. Strain metrics are
  crucial for evaluating the physical or mechanical stress imparted by the calibration on the materials or instruments
  involved.

- strainStandardDeviation (float): Indicates the standard deviation of strain measurements, offering a gauge of variability
  in the strain induced by the calibration.

- twoThetaAverage (float): Averages the two-theta angle, typically measured in degrees, observed across the calibration
  dataset. The two-theta angle is fundamental in diffraction measurements, and its average value can indicate the
  calibration's alignment accuracy.
