### Algorithm: `LoadGroupingDefinition`

#### Description:
This algorithm creates a grouping workspace from a grouping definition file.
It supports NEXUS, XML, and HDF formats and uses an associated instrument
specified by various means.

#### Expected Inputs:
1. **GroupingFilename**:
   - **Type**: `FileProperty`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: Path to the grouping file to be loaded.

2. **InstrumentName**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Name of an associated instrument.

3. **InstrumentFilename**:
   - **Type**: `String`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Path of an associated instrument definition file.

4. **InstrumentDonor**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Input`
   - **Property Mode**: `Optional`
   - **Description**: Workspace to optionally take the instrument from, when GroupingFilename is in XML format.

#### Expected Outputs:
1. **OutputWorkspace**:
   - **Type**: `MatrixWorkspace`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: Name of an output grouping workspace.
