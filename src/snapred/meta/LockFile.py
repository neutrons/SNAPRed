import os
import socket
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel

from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


def hostName():
    """Get the hostname of the current machine."""
    return socket.gethostname().split(".")[0]


def _reapOldLockfiles(lockfilePath: Path, maxAgeSeconds: int, lockedPath: Path):
    # check if any lockfile exists
    existing_lockfiles = list(lockfilePath.parent.glob("*.lock"))
    if existing_lockfiles:
        # sort by creation time and remove ones older than 10 seconds
        existing_lockfiles = [f for f in existing_lockfiles if time.time() - f.stat().st_ctime > maxAgeSeconds]
        for lockfile in existing_lockfiles:
            lockfile.unlink()

        # if still exists, raise error
        remaining_lockfiles = list(lockfilePath.parent.glob("*.lock"))

        # filter for lockfiles that contain the lockedPath
        remaining_lockfiles = [f for f in remaining_lockfiles if str(lockedPath) in f.read_text().splitlines()]

        if remaining_lockfiles:
            if len(remaining_lockfiles) > 1:
                raise RuntimeError("Multiple lockfiles found, cannot proceed.")
            remainingLockFile = remaining_lockfiles[0]
            remainingPid = remainingLockFile.name.split("_")[0]
            remainingHost = remainingLockFile.name.split("_")[1]
            # NOTE: This will not support the application doing many writes in parallel
            #       if necessary, the old lockfile contents should be copied.
            if remainingPid == str(os.getpid()) and remainingHost == hostName():
                remainingLockFile.unlink()
            else:
                return False
    return True


def _generateLockfile(lockedPath: Path):
    # get pid
    pid = str(os.getpid())
    lockFileRoot = Config["lockfile.root"]
    lockFilePath = Path(f"{lockFileRoot}/{pid}_{hostName()}_{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}.lock")
    if not lockFilePath.parent.exists():
        lockFilePath.parent.mkdir(parents=True, exist_ok=True)
    # check if any lockfile exists
    maxAgeSeconds = Config["lockfile.ttl"]  # seconds
    timeout = Config["lockfile.timeout"]  # seconds
    while not _reapOldLockfiles(lockFilePath, maxAgeSeconds, lockedPath):
        # if the lockfile still exists, wait for it to be removed
        time.sleep(maxAgeSeconds)
        timeout -= maxAgeSeconds
        if timeout <= 0:
            raise RuntimeError(f"Timeout waiting for lockfile {lockFilePath} to be removed.")
    # create new lockfile
    lockFilePath.touch(exist_ok=True)
    # append lockedPath to the lockfile
    with lockFilePath.open("a") as lockFile:
        lockFile.write(str(lockedPath.expanduser().resolve()) + "\n")

    return lockFilePath


@contextmanager
def LockManager(lockedPath: Path):
    # Context manager to safely lock a dir for a specific type of operation.

    # __enter__
    # generate a lockfile
    lockFile = None
    try:
        lockFile = LockFile(lockedPath)
        yield lockFile
    finally:
        # __exit__
        if bool(lockFile) and lockFile.exists():
            lockFile.release()


class LockFile(BaseModel):
    lockFilePath: Path

    def __init__(self, lockedPath: Path):
        super().__init__(lockFilePath=_generateLockfile(lockedPath))

    def exists(self) -> bool:
        """Check if the lock file exists."""
        return self.lockFilePath is not None and self.lockFilePath.exists()

    def release(self):
        if self.lockFilePath and self.lockFilePath.exists():
            self.lockFilePath.unlink()
            self.lockFilePath = None
        else:
            logger.warning("Attempted to release a lock file that does not exist or has already been released.")
