from pydantic import BaseModel


class RegisterNeutronDataRequest(BaseModel):
    """Register a workspace as the neutron data for a run number."""

    runNumber: str
    workspace: str
