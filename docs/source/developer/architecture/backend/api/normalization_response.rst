NormalizationResponse Class Documentation
=========================================

NormalizationResponse is a class extending BaseModel from the pydantic library, designed to represent the outcomes of a vanadium-based normalization
procedure comprehensively. It encapsulates the corrected, focused, and smoothed states of vanadium data, along with detected peaks across detectors,
providing a structured overview of the results. This class plays a crucial role in facilitating further analysis or reporting within scientific
workflows by detailing each state of vanadium data and the associated peak lists, making it essential for evaluating and making informed decisions
post-normalization.


Attributes:
-----------

- correctedVanadium (str): The identifier or file path of the vanadium after correction
  procedures have been applied. These adjustments are typically made to account for
  instrument or experimental errors, ensuring the vanadium data is as accurate as
  possible for subsequent analyses.

- focusedVanadium (str): The identifier or file path of the vanadium post-focus processing,
  indicating the vanadium data has been concentrated or enhanced for particular regions
  or aspects of interest. This process aims to highlight specific features or
  characteristics of the vanadium for detailed study.

- smoothedVanadium (str): The identifier or file path of the vanadium data after smoothing
  operations have been performed, aimed at reducing noise and improving signal clarity.
  Smoothing is crucial for enhancing the quality of the data, making it more suitable
  for analysis and interpretation.

- detectorPeaks (List[GroupPeakList]): A list of GroupPeakList instances, each representing
  a collection of peaks detected across different detector groups or configurations
  during the normalization process. These detected peaks are critical for understanding
  the material's response under investigation, facilitating comprehensive analysis and
  characterization.
