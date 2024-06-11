Algorithm: `SaveGroupingDefinition`
===================================

Description:
------------
This algorithm takes in a grouping definition (file or object) and saves it in a diffraction
calibration file format. Either a grouping filename or a grouping workspace must be specified,
but not both.

Expected Inputs:
----------------
1. **GroupingFilename**:
    - **Type**: `FileProperty`
    - **Direction**: `Input`
    - **Property Mode**: `Optional`
    - **Description**: Path of an input grouping file (in NEXUS or XML format).

2. **GroupingWorkspace**:
    - **Type**: `MatrixWorkspace`
    - **Direction**: `Input`
    - **Property Mode**: `Optional`
    - **Description**: Name of an input grouping workspace.

3. **OutputFilename**:
    - **Type**: `FileProperty`
    - **Direction**: `Input`
    - **Property Mode**: `Mandatory`
    - **Description**: Path of an output file. Supported file name extensions: "h5", "hd5", "hdf".

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
    - **Type**: `MatrixWorkspace`
    - **Direction**: `Input`
    - **Property Mode**: `Optional`
    - **Description**: Workspace to optionally take the instrument from, when GroupingFilename is in XML format.
