Algorithm: `DetectorPeakPredictor`
==================================

Description:
------------
This algorithm predicts locations based on crystallographic information, and predicts
peak widths based on resolution estimates of the instrument. It calculates the beta
terms, full width at half maximum (FWHM), and generates a list of `DetectorPeak`
objects for each group, optionally purging duplicates.

Expected Inputs:
----------------
1. **Ingredients**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: The detector peak ingredients.

2. **PurgeDuplicates**:
   - **Type**: `Bool`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Flag to indicate whether to purge duplicate peaks.

Expected Outputs:
-----------------
1. **DetectorPeaks**:
   - **Type**: `String`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: The returned list of `GroupPeakList` objects.
