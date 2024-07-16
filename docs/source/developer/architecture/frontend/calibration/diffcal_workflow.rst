DiffCalWorkflow Class Documentation
===================================

The DiffCalWorkflow class is a orchestration of the diffraction calibration process within the SNAPRed application.
It structures the calibration workflow into a sequence of distinct steps, utilizing a set of specialized views
(DiffCalRequestView, DiffCalTweakPeakView, DiffCalAssessmentView, and DiffCalSaveView) to guide the user through
calibration setup, peak adjustment, assessment, and the optional saving of calibration results.


Workflow Overview:
------------------

- Initialization: The workflow begins by setting up necessary configurations, including loading calibration
  parameters and sample paths. It dynamically adjusts to the user's inputs, such as run number and calibration
  settings, ensuring a tailored calibration experience.

- User Interaction and Data Input: Through DiffCalRequestView, users input initial calibration parameters such
  as run number, lite mode preference, and sample selection. This step lays the groundwork for subsequent
  calibration actions.

- Peak Adjustment: Utilizing DiffCalTweakPeakView, users can adjust calibration peaks based on real-time graphical
  feedback. This interactive step allows for precise calibration parameter tuning.

- Calibration Assessment: The DiffCalAssessmentView facilitates the review of calibration results, enabling users
  to assess the calibration quality and decide on further actions, whether to iterate for refinement or proceed to save.

- Calibration Saving: In the final step, DiffCalSaveView provides users with the option to save the calibration data,
  completing the calibration process. It collects necessary metadata for saving, including authorship and comments.
