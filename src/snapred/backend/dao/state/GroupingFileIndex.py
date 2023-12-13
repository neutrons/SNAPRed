from pydantic import BaseModel

from pathlib import Path

class GroupingFileIndex(BaseModel):
    groupingName: str
    filePath: Path