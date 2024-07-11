from pydantic import BaseModel, model_validator

from snapred.backend.dao.request.CreateIndexEntryRequest import CreateIndexEntryRequest
from snapred.backend.dao.request.CreateNormalizationRecordRequest import CreateNormalizationRecordRequest


class NormalizationExportRequest(BaseModel):
    """

    This class is utilized to encapsulate the necessary data for saving a completed normalization
    process to disk, following a satisfactory assessment by the user. It packages both the
    comprehensive details of the normalization process and its contextual metadata, ensuring that
    significant normalization efforts are archived in a structured and accessible manner. This
    approach facilitates not only the preservation of critical scientific data but also supports
    data governance, compliance, and reproducibility within the research workflow.

    """

    createIndexEntryRequest: CreateIndexEntryRequest
    createRecordRequest: CreateNormalizationRecordRequest

    @model_validator(mode="after")
    def same_version(self):
        if self.createIndexEntryRequest.version != self.createRecordRequest.version:
            raise ValueError(
                f"Version {self.createIndexEntryRequest.version} does not match {self.createRecordRequest.version}"
            )
        else:
            return self
