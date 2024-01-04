from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


def test_collectWarnings():
    logger.warn("Warning 1")
    logger.warn("Warning 2")
    logger.warn("Warning 3")
    assert snapredLogger.hasWarnings()
    assert len(snapredLogger.getWarnings()) == 3
    snapredLogger.clearWarnings()
    assert not snapredLogger.hasWarnings()
