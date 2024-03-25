DiffCalTweakPeakView Class Documentation
========================================

DiffCalTweakPeakView is specifically tailored for the adjustment and visualization of diffraction
calibration peaks. It is adorned with the Resettable decorator to ensure adaptability to changing
calibration needs. This view incorporates a blend of user input mechanisms, including fields for
calibration parameters, interactive controls for precise adjustment, and graphical display elements
for immediate visualization of changes.


Structure and Functionality:
----------------------------

- Run Number and Lite Mode Toggle: Initial setup includes a field for entering the run number and
  a toggle for lite mode operation, aligning the calibration process with user-specific requirements.

- Graphical Elements: A matplotlib graph embedded within the view offers real-time visualization of
  calibration peaks, enabling users to assess the impact of adjustments visually. This is complemented
  by a navigation toolbar for enhanced graph interaction.

- Adjustment Controls: Input fields for defining minimum and maximum d-spacing (dMin, dMax) and intensity
  threshold provide users with the means to fine-tune calibration peaks. These adjustments are facilitated
  through a straightforward interface, promoting accuracy and ease of use.

- Recalculate Button: An essential feature that triggers the recalibration process based on the newly
  specified parameters, highlighting the view's interactive and responsive design.
