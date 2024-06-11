### Algorithm: `CrystallographicInfoAlgorithm`

#### Description:
This algorithm ingests crystallographic information from a CIF file or a provided
crystallography JSON string. It generates unique reflections within a specified
d-spacing range, calculates the d-values and structure factors (F^2), and
packages this information into a `CrystallographicInfo` object.

#### Expected Inputs:
1. **CifPath**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Path to the CIF file containing crystallographic information.

2. **Crystallography**:
   - **Type**: `String`
   - **Direction**: `InOut`
   - **Property Mode**: `Mandatory`
   - **Description**: JSON string defining the crystallography data.

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

#### Expected Outputs:
1. **CrystalInfo**:
   - **Type**: `String`
   - **Direction**: `Output`
   - **Property Mode**: `Optional`
   - **Description**: JSON string containing the extracted crystallographic information.
