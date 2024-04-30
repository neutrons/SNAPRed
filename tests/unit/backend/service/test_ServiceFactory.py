from snapred.backend.service.ServiceFactory import ServiceFactory


def test_serviceFactory():
    # verify a base number of services
    servicenames = ServiceFactory().getServiceNames()
    assert len(servicenames) > 5

    # pick out a couple of services that should always be around
    assert "api" in servicenames
    assert "config" in servicenames
    # assert "reduction" in servicenames
    assert "stateId" in servicenames
    assert "calibration" in servicenames
