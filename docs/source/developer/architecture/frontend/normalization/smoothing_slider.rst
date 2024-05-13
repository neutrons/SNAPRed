SmoothingSlider Class Documentation
===================================

SmoothingSlider is a custom qt QWidget crafted to provide an enhanced interface for adjusting a smoothing parameter, skillfully integrating a
graphical slider with a numerical input field. This combination caters to diverse user preferences, allowing for both rapid visual adjustments via
the slider and precise value entry through the numerical input. It's particularly suited for applications requiring detailed parameter settings,
offering users a versatile tool for fine-tuning smoothing levels.


Key Features:
-------------

- Dual Interface: Features a QSlider for intuitive graphical adjustments and a QLineEdit for direct numerical entry, accommodating both quick
  explorations and specific parameter settings.

- Custom Value Mapping: Implements logarithmic mapping of the slider's value range (from -1000 to 0) to enable a wide and granular control over the
  smoothing parameter values.

- Styling: Customizes the slider's appearance with CSS styling, enhancing the visual interaction experience.

- Validation and Error Handling: Ensures the integrity of the entered values, providing user feedback through warning messages for non-numeric or
  out-of-range inputs.


Functionalities:
----------------

- Value Conversion: Utilizes a logarithmic conversion between the slider value and the numerical input, facilitating an intuitive and
  resolution-friendly adjustment mechanism for the smoothing parameter.

- Synchronization Between Controls: Keeps the slider and text field in sync, ensuring that adjustments in one are accurately reflected in the other.
  This feature maintains consistency and accuracy, preventing user confusion and input errors.

- Error Handling: Enforces input validation, restricting entries to non-negative numbers and presenting users with warnings for invalid inputs. This
  approach helps safeguard the application against erroneous parameter manipulations.

- Programmatic Value Setting: Offers methods like setValue for programmatically updating the smoothing parameter, making it straightforward to
  integrate the widget into broader workflows or automated processes.


SmoothingSlider significantly enriches the user interface by providing a responsive and intuitive control for smoothing parameter adjustments.
Through its thoughtfully designed features and functionalities, it enhances the usability of applications, enabling users to achieve optimal
smoothing settings with ease and precision.
