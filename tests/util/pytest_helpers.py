from typing import List
import time

import pytest

from mantid.simpleapi import DeleteWorkspaces, mtd

# Import required test fixtures at the end of either the main `conftest.py`,
#   or any `conftest.py` at the test-module level directory.


## WARNING:
#  * The following two methods duplicate code, however the _closures_ need to be distinct!

@pytest.fixture(scope="function")  # noqa: PT003
def cleanup_workspace_at_exit():
    # Allow cleanup of workspaces in the ADS
    #   in a manner compatible with _parallel_ testing.
    _workspaces: List[str] = []

    def _cleanup_workspace_at_exit(wsName: str):
        _workspaces.append(wsName)

    yield _cleanup_workspace_at_exit

    # teardown
    try:
        if _workspaces:
            # Warning: `DeleteWorkspaces`' input validator throws an exception
            #   if a specified workspace doesn't exist in the ADS;
            
            # Provide an error diagnostic message, but do not bypass the error:
            #     the workspaces list must be correct.
            non_existent_workspaces = set([ws for ws in _workspaces if not mtd.doesExist(ws)])
            print(f"Non-existent workspaces: {non_existent_workspaces}.")            
            
            DeleteWorkspaces(_workspaces)
    except RuntimeError:
        pass


@pytest.fixture(scope="class")
def cleanup_class_workspace_at_exit():
    # Allow cleanup of workspaces in the ADS
    #   in a manner compatible with _parallel_ testing.
    _workspaces: List[str] = []

    def _cleanup_workspace_at_exit(wsName: str):
        _workspaces.append(wsName)

    yield _cleanup_workspace_at_exit

    # teardown
    try:
        if _workspaces:
            # Warning: `DeleteWorkspaces`' input validator throws an exception
            #   if a specified workspace doesn't exist in the ADS;
            
            # Provide an error diagnostic message, but do not bypass the error:
            #     the workspaces list must be correct.
            print(f"Non-existent workspaces: {set([ws for ws in _workspaces if not mtd.doesExist(ws)])}.")            

            DeleteWorkspaces(_workspaces)
    except RuntimeError:
        pass

@pytest.fixture()
def get_unique_timestamp():
    """
    This method re-uses code from `LocalDataService.getUniqueTimestamp`.
    
    Generate a unique timestamp:

    * on some operating systems `time.time()` only has resolution to seconds;

    * this method checks its own most-recently returned value, and if necessary,
      increments it.

    * the complete `float` representation of the unix timestamp is retained,
      in order to allow arbitrary formatting.

    """
    _previousTimestamp = None
    
    def _get_unique_timestamp() -> float:
        nextTimestamp = time.time()
        if _previousTimestamp is not None:
            # compare as `time.struct_time`
            if nextTimestamp < _previousTimestamp or time.gmtime(nextTimestamp) == time.gmtime(_previousTimestamp):
                nextTimestamp = _previousTimestamp + 1.0
        _previousTimestamp = nextTimestamp
        return nextTimestamp
        
    yield _get_unique_timestamp

    # teardown ...
    pass
