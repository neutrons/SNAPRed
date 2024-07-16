from pydantic import BaseModel


class FocusGroup(BaseModel):
    # name of grouping schema:
    # e.g. "Column", "Bank", "All"
    name: str

    # relative or absolute file path:
    # * a relative path here is relative to <instrument.calibration.powder.grouping.home>
    definition: str
