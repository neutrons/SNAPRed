from pydantic import BaseModel


class UnregisterNeutronDataRequest(BaseModel):
    """Remove the registration of neutron data for a run number."""

    runNumber: str
