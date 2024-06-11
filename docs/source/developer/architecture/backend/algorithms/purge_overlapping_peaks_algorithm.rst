Algorithm: `PurgeOverlappingPeaksAlgorithm`
===========================================

Description:
------------
This algorithm purges overlapping peaks from a list of peaks based on intensity and d-spacing range. It
ensures that only non-overlapping peaks within a specified d-spacing range and above a specified
intensity threshold are retained.

Expected Inputs:
----------------
1. **Ingredients**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: The CrystalInfo and Intensity Threshold ingredients.

2. **DetectorPeaks**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Input list of peaks.

3. **dMin**:
   - **Type**: `Float`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Minimum d-spacing for peak selection.

4. **dMax**:
   - **Type**: `Float`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Maximum d-spacing for peak selection.

Expected Outputs:
-----------------
1. **OutputPeakMap**:
   - **Type**: `String`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: The resulting, non-overlapping list of peaks.
