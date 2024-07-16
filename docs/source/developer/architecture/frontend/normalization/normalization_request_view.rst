NormalizationRequestView Class Documentation
============================================

NormalizationRequestView is a user interface class within SNAPRed, crafted to enhance the process of submitting normalization requests. By extending BackendRequestView and incorporating the @Resettable decorator, this class provides a streamlined and user-friendly experience for managing normalization workflows. It integrates various UI components such as run number fields, lite mode toggles, and dropdown menus for sample and grouping file selection, ensuring that users can effortlessly input the necessary data for normalization requests.


Key Components and Functionalities:
-----------------------------------

- Initialization: The class initializes with paths for calibration and diffraction calibration, utilizing UI elements defined in a JSON form
  structure. This setup facilitates the incorporation of specific UI components tailored to the normalization request process.

- User Input Facilitation: Through fields for run numbers, a lite mode toggle, and background run numbers, the class leverages data from jsonForm to
  collect essential user inputs. Additionally, it offers dropdown menus for selecting samples and grouping files, populated with samplePaths and
  groups, respectively.

- UI Element Arrangement: The layout is designed thoughtfully to optimize the user experience, arranging UI elements in a manner that is intuitive
  and conducive to efficient data entry.


UI Elements:
------------

- Run Number Field: Enables users to input the run identifier, a crucial parameter for the normalization process.

- Lite Mode Toggle: A toggle switch provided to enable or disable lite mode. Lite mode optimizes resource usage during the normalization process.

- Background Run Number Field: Allows for the input of the background run's identifier, necessary for certain normalization operations.

- Sample Dropdown: Facilitates sample selection from a list of predefined paths, simplifying the process of specifying the sample to be normalized.

- Grouping File Dropdown: Offers a mechanism for selecting a grouping file from available options, essential for the normalization process.


Functions:
----------

- populateGroupingDropdown(groups): Dynamically updates the items in the grouping file dropdown, allowing for the selection from updated or newly
  available grouping files.

- verify(): Conducts a validation check on user inputs to ensure all required fields are adequately filled before proceeding with the submission of a
  normalization request. This function raises informative errors if validation fails, guiding the user to rectify input issues.

- getRunNumber(): Retrieves the run number input by the user, an essential piece of data for processing the normalization request.


This class serves as a pivotal bridge between user inputs and the backend normalization request process in SNAPRed. It ensures that users can
initiate normalization operations with ease, contributing significantly to a seamless and intuitive user experience in scientific data processing and
analysis workflows.
