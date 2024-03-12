from typing import List

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList


class NormalizationResponse(BaseModel):
    """
    Defines a Normalization Response object class with attributes representing the outcomes of
    a vanadium-based normalization procedure.

    Attributes:
        correctedVanadium (str): The identifier or file path of the vanadium after correction
            procedures have been applied. These adjustments are typically made to account for
            instrument or experimental errors, ensuring the vanadium data is as accurate as
            possible for subsequent analyses.
        focusedVanadium (str): The identifier or file path of the vanadium post-focus processing,
            indicating the vanadium data has been concentrated or enhanced for particular regions
            or aspects of interest. This process aims to highlight specific features or
            characteristics of the vanadium for detailed study.
        smoothedVanadium (str): The identifier or file path of the vanadium data after smoothing
            operations have been performed, aimed at reducing noise and improving signal clarity.
            Smoothing is crucial for enhancing the quality of the data, making it more suitable
            for analysis and interpretation.
        detectorPeaks (List[GroupPeakList]): A list of GroupPeakList instances, each representing
            a collection of peaks detected across different detector groups or configurations
            during the normalization process. These detected peaks are critical for understanding
            the material's response under investigation, facilitating comprehensive analysis and
            characterization.

    This class serves as a structured representation of the outcomes from a vanadium-based
    normalization procedure, encapsulating the various states of vanadium data (corrected,
    focused, and smoothed) alongside detected peaks across detectors. It provides a comprehensive
    overview of the results, facilitating further analysis or reporting within scientific
    workflows. The detailed encapsulation of each state of vanadium data and the collected peak
    lists make this class an invaluable asset for post-normalization process evaluation and
    decision-making.
    """

    correctedVanadium: str
    focusedVanadium: str
    smoothedVanadium: str
    detectorPeaks: List[GroupPeakList]
