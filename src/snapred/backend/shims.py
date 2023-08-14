from contextlib import contextmanager
from copy import deepcopy

from mantid.kernel import ConfigService


@contextmanager
def amend_mantid_config(new_config=None, data_dir=None):
    r"""
    Context manager to safely modify Mantid Configuration Service while
    the function is executed.

    This is taken from drtsans

    Parameters
    ----------
    new_config: dict
        (key, value) pairs to substitute in the configuration service
    data_dir: str, list
        Append one (when passing a string) or more (when passing a list)
        directories to the list of data search directories.
    """
    modified_keys = list()
    backup = dict()
    config = ConfigService.Instance()
    if new_config is not None:
        SEARCH_ARCHIVE = "datasearch.searcharchive"
        if SEARCH_ARCHIVE not in new_config:
            new_config[SEARCH_ARCHIVE] = "hfir, sns"
        DEFAULT_FACILITY = "default.facility"
        if DEFAULT_FACILITY not in new_config:
            new_config[DEFAULT_FACILITY] = "SNS"
        for key, val in new_config.items():
            backup[key] = config[key]
            config[key] = val  # config does not have an 'update' method
            modified_keys.append(key)
    if data_dir is not None:
        data_dirs = (
            [
                data_dir,
            ]
            if isinstance(data_dir, str)
            else data_dir
        )
        key = "datasearch.directories"
        backup[key] = deepcopy(config[key])
        # prepend our custom data directories to the list of data search directories
        config.setDataSearchDirs(data_dirs + list(config.getDataSearchDirs()))
    try:
        yield
    finally:
        for key in modified_keys:
            config[key] = backup[key]
