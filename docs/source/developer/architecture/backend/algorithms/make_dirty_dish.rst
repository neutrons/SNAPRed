Algorithm: `MakeDirtyDish`
==========================

Description:
------------
This algorithm records a workspace in a state for the CIS to view later by
cloning the input workspace to an output workspace if CIS mode is enabled.

Expected Inputs:
----------------
1. **InputWorkspace**:
   - **Type**: `Workspace`
   - **Direction**: `Input`
   - **Property Mode**: `Mandatory`
   - **Description**: The workspace to be cloned.

Expected Outputs:
-----------------
1. **OutputWorkspace**:
   - **Type**: `Workspace`
   - **Direction**: `Output`
   - **Property Mode**: `Mandatory`
   - **Description**: The cloned workspace.
