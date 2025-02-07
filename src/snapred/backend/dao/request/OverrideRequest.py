from pydantic import BaseModel


class OverrideRequest(BaseModel, extra="forbid"):
    calibrantSamplePath: str
