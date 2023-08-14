def list_to_raw(baseModelList):
    import json

    return json.dumps([baseModel.dict() for baseModel in baseModelList])
