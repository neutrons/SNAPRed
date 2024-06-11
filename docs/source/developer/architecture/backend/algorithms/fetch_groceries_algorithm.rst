### Algorithm: `FetchGroceriesAlgorithm`

#### Description:
This algorithm is used for general-purpose loading of Nexus data or grouping workspaces.
It supports various loader types and handles different file formats.

#### Expected Inputs:
1. **Filename**:
   - **Type**: `FileProperty`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Path to the file to be loaded. Supported extensions are `xml`, `h5`, `nxs`, `hd5`.

2. **LoaderType**:
   - **Type**: `String`
   - **Direction**: `InOut`
   - **Property Mode**: `Mandatory`
   - **Description**: Type of loader to use. Valid options include `LoadGroupingDefinition`, `LoadCalibrationWorkspaces`, `LoadNexus`, `LoadEventNexus`, `LoadNexusProcessed`, `ReheatLeftovers`.

3. **LoaderArgs**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Loader keyword arguments in JSON format.

4. **InstrumentName**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Name of an associated instrument.

5. **InstrumentFilename**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Path of an associated instrument definition file.

6. **InstrumentDonor**:
   - **Type**: `WorkspaceProperty`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Workspace to optionally take the instrument from.

#### Expected Outputs:
1. **OutputWorkspace**:
   - **Type**: `Workspace`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: Workspace containing the loaded data.
