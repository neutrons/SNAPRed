import pytest
from snapred.ui.view.InitializeStateCheckView import InitializationMenu


@pytest.mark.ui()
def test_run_number_field(qtbot):
    menu = InitializationMenu(None)
    qtbot.addWidget(menu)

    assert menu.getRunNumber() == ""

    test_value = "1234"
    qtbot.keyClicks(menu.runNumberField, test_value)
    assert menu.getRunNumber() == test_value
