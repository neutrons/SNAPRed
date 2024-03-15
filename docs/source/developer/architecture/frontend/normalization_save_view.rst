NormalizationSaveView Class Documentation
=========================================

NormalizationSaveView is a PyQt5 widget interface developed for the efficient handling of normalization data saving in SNAPRed after user assessment.
Decked with the @Resettable decorator, it furnishes users with a systematic and intuitive platform for inputting and reviewing critical details such
as run numbers and versioning information. Through the dynamic generation of forms and the organized presentation of UI elements, it ensures data
coherence and simplifies user interaction. The principal aim is to refine the process of confirming and saving normalization data, thereby elevating
both the user experience and the reliability of the stored data.


Components and Functionalities:
-------------------------------

- Inheritance: Derives from QWidget, creating a graphical interface component that forms the foundation of the user interface.

- Dynamic Form Generation: Leverages JsonFormList to dynamically create form fields based on a JSON schema map. This approach promotes data
  consistency and streamlines the validation process.

- Layout Management: Utilizes a QGridLayout for the efficient organization of UI elements, ensuring an ergonomic layout that enhances the user
  experience.


UI Elements:
------------

- Interaction Text: A QLabel that informs the user about the assessment completion and queries their intent to save the normalization data.

- Field Elements: Consist of LabeledField widgets for the entry and display of normalization-related information, including run numbers, background
  run numbers, version, applicability, comments, and authorship. Specific fields are made non-editable or accompanied by tooltips to assist users in
  providing accurate information.

- Signal-Slot Mechanism: Incorporates pyqtSignal for thread-safe updates to UI components related to run number adjustments. The mechanisms ensure
  that UI updates are handled efficiently and without risk to the application's stability.


Signal-Slot Mechanism:
----------------------

- Signal Definitions: Includes pyqtSignal instances for the thread-safe updating of UI elements pertinent to run numbers and background run numbers.

- Slots: Defines slots such as _updateRunNumber and _updateBackgroundRunNumber which are connected to signals. These slots receive data to update the
  UI elements with new values, facilitating seamless and secure interactions within the UI.

- NormalizationSaveView significantly streamlines the process of normalization data saving, providing a clear and effective interface for users to
  review and confirm normalization details prior to saving. This class is instrumental in ensuring a smooth transition from data assessment to data
  persistence, thereby bolstering data integrity and enhancing user satisfaction within the SNAPRed platform.
