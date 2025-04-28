from pydantic import BaseModel, ConfigDict


class PreprocessReductionIngredients(BaseModel):
    pass

    model_config = ConfigDict(
        extra="forbid",
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
