import pytest

# from snapred.backend.service.ExtractionService import ExtractionService


def setup():
    """Setup before all tests"""
    pass


def teardown():
    """Teardown after all tests"""
    pass


@pytest.fixture(autouse=True)
def setup_teardown():  # noqa: PT004
    """Setup before each test, teardown after each test"""
    setup()
    yield
    teardown()


def test_useless():
    pass


# def mockedExtractionService():
#     service = ExtractionService()
#     service.dataFactoryService = mock.Mock()
#     return service


# def test_init():
#    service = mockedExtractionService()
#    assert service is not None
