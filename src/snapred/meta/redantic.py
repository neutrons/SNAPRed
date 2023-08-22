import json


def list_to_raw(baseModelList):
    return json.dumps([baseModel.dict() for baseModel in baseModelList])


def list_to_raw_pretty(baseModelList):
    return json.dumps([baseModel.dict() for baseModel in baseModelList], indent=4)


def write_model(baseModel, path):
    with open(path, "w") as f:
        f.write(baseModel.json())


def write_model_pretty(baseModel, path):
    with open(path, "w") as f:
        f.write(baseModel.json(indent=4))


def write_model_list(baseModelList, path):
    with open(path, "w") as f:
        f.write(list_to_raw(baseModelList))


def write_model_list_pretty(baseModelList, path):
    with open(path, "w") as f:
        f.write(list_to_raw_pretty(baseModelList))
