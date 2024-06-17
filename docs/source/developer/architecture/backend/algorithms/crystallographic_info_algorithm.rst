Algorithm: `CrystallographicInfoAlgorithm`
==========================================

Description:
------------
This algorithm ingests `Crystallography` information from a CIF file or a provided
crystallography JSON string. It generates unique reflections by parsing in a mantid
`CrystalStructure` object. Then given a specified d-spacing range, it calculates
the d-values and structure factors (F^2), and packages this information into a
`CrystallographicInfo` object.

Expected Inputs:
----------------
1. **CifPath**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Path to the CIF file containing crystallographic information.

2. **Crystallography**:
   - **Type**: `String`
   - **Direction**: `InOut`
   - **Property Mode**: `Mandatory`
   - **Description**: SNAPRed DAO pydantic object containing crystallographic information.

3. **dMin**:
   - **Type**: `Float`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Minimum d-spacing for reflection generation. Default value is set from configuration.

4. **dMax**:
   - **Type**: `Float`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Maximum d-spacing for reflection generation. Default value is set from configuration.

Expected Outputs:
-----------------
1. **CrystalInfo**:
   - **Type**: `String`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: JSON string containing the extracted crystallographic information.
