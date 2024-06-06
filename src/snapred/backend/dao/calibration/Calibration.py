from snapred.backend.dao.state.StateParameters import StateParameters

# NOTE: the __init__ loads CalibrationExportRequest, which imports Calibration, which causes
#       a circular import situation.  In the future, need to remove the circualr import
#       so that full set of ingredients can be preserved
# from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients


class Calibration(StateParameters):
    """

    The Calibration class acts as a container for parameters primarily utilized in fitting processes within the context
    of scientific data analysis. It encompasses static details such as the instrumentState indicating the condition of
    the instrument at the time of calibration, seedRun for identifying the initial data set, creationDate marking when
    the calibration was created, along with a name and a default version number.

    """

    pass
