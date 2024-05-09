NormalizationTweakPeakView Class Documentation
==============================================

NormalizationTweakPeakView is a qt-based GUI component crafted for the precise adjustment of peak normalization parameters within SNAPRed. By
inheriting from BackendRequestView and featuring an array of interactive components such as input fields, dropdowns, sliders, and a real-time
matplotlib plot area, it offers a comprehensive environment for dynamic interaction and visualization. This GUI is designed to allow users to
instantly observe the effects of their parameter adjustments on normalization settings, thereby enhancing the analytical capabilities and user
experience in data processing tasks.


Structure and Features:
-----------------------

- Initialization: Configures the UI layout and establishes signal-slot connections for asynchronous UI updates, including the integration of a
  matplotlib canvas for the visualization of normalization data and detected peaks.

- Run Number and Background Run Number Fields: Initializes fields for inputting run numbers, with an option to disable them to prevent unintended
  modifications during the adjustment process.


UI Components:
--------------

- Graphical Elements: Incorporates a matplotlib figure to facilitate the plotting of data, enabling users to visually compare and assess
  normalization adjustments and peak detections.

- Input Fields and Controls: Features input fields for run numbers, dropdown menus for sample and grouping file selection, and sliders for \
  fine-tuning parameters such as the smoothing level, dMin, dMax, and peak intensity threshold.

- Action Buttons: Includes a "Recalculate" button, empowering users to apply their adjustments and immediately observe the impacts on the
  normalization process and peak detection outcomes.


Interactivity and Signal Handling:
----------------------------------

- PyQt Signals: Employs PyQt signals to manage UI actions, linking user interactions with corresponding methods for a seamless and responsive user
  experience. This mechanism facilitates the real-time update of UI components in response to user inputs or changes in external data.


Data Visualization and Adjustment:
----------------------------------

- Dynamic Plot Updates: Responds to parameter adjustments by updating the plot area, offering instant visual feedback on the implications of the
  changes. This functionality ensures a clear and informative presentation of both data and detected peaks within the plot area.


Validation and Warnings:
------------------------

- Parameter Validation: Implements input validation to ensure the integrity of parameter settings. It also provides warnings for potential issues,
  such as improper parameter values, guiding users toward suitable adjustments and preventing incorrect processing requests.


NormalizationTweakPeakView significantly elevates the process of fine-tuning normalization parameters, presenting an efficient tool for interactive
data analysis and optimization. Through its detailed and intuitive interface, it facilitates an enhanced user experience in adjusting and visualizing
normalization settings, contributing to the accuracy and effectiveness of data analysis within SNAPRed.
