import shutil
from pathlib import Path

import pytest
from cis_tests.diffcal_integration_test_script import script as test_script

from snapred.meta.Config import Config, Resource


@pytest.fixture(autouse=True)
def _cleanup_directories():
    # WORKAROUND: On test failure: pytest doesn't call `state_root_override` context manager __exit__.
    stateId = "04bd2c53f6bf6754"
    stateRootPath = Path(Config["instrument.calibration.powder.home"]) / stateId
    if stateRootPath.exists():
        raise RuntimeError(f"state root directory '{stateRootPath}' already exists -- please move it out of the way")
    yield

    # teardown
    if stateRootPath.exists():
        shutil.rmtree(stateRootPath)


@pytest.mark.skip(
    reason="TODO: integrate treatment of state-root directory with "
    + "that used by 'tests/integration/test_workflow_panels_happy_path.py'"
)
@pytest.mark.golden_data(
    path=Resource.getPath("outputs/integration/diffcal/golden_data"), short_name="diffcal", date="2024-04-24"
)
@pytest.mark.integration
def test_diffcal(goldenData):
    # to launch, use either of:
    #   * `env=integration_test pytest -m integration`, or
    #   * `env=dev_test pytest -m integration`, where "environment: integration_test" is present
    #   in the "dev_test.yml" file. Note that "test" must be part of the "*.yml" filename.
    assert Config["environment"] == "integration_test"
    test_script(goldenData)
