import os
import socket
import time
from contextlib import contextmanager
from pathlib import Path

from pydantic import BaseModel

from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


def hostName():
    """Get the hostname of the current machine."""
    return socket.gethostname().split(".")[0]


def _removePath(lockFilePath: Path, lockedPath: Path):
    """Remove the lockedPath from the lock file."""
    lockedPath = lockedPath.expanduser().resolve()
    if lockFilePath.exists():
        with lockFilePath.open("r") as lockFile:
            lines = lockFile.readlines()
        # remove first appearance of lockedPath
        lines.pop(next((i for i, line in enumerate(lines) if str(lockedPath) == line.strip()), None))
        with lockFilePath.open("w") as lockFile:
            lockFile.writelines(lines)


def _getApplicableLockfilePaths(lockfilePath: Path, lockedPath: Path) -> Path:
    """Get the lockfiles that contain lockedPath."""
    existingLockfiles = list(lockfilePath.parent.glob("*.lock"))
    lockedPath = lockedPath.expanduser().resolve()
    applicableLockfiles = []
    if existingLockfiles:
        # filter for lockfiles that contain the lockedPath
        applicableLockfiles = [f for f in existingLockfiles if str(lockedPath) in f.read_text().splitlines()]
    return applicableLockfiles


def _generateLockfileName() -> str:
    """Generate a lockfile name based on the pid and host."""
    pid = str(os.getpid())
    host = hostName()
    return f"{pid}_{host}.lock"


def _reapOldLockfiles(lockfilePath: Path, maxAgeSeconds: int, lockedPath: Path):
    """
    Reap lockfiles that are older than maxAgeSeconds.
    Returns True if process can initiate lock, False if it cannot.
    """
    # check if any lockfile exists
    existingLockfiles = list(lockfilePath.parent.glob("*.lock"))
    lockedPath = lockedPath.expanduser().resolve()
    if existingLockfiles:
        # reap files older than 10 seconds
        existingLockfiles = [f for f in existingLockfiles if time.time() - f.stat().st_ctime > maxAgeSeconds]
        for lockfile in existingLockfiles:
            lockfile.unlink(missing_ok=True)

        remainingLockfiles = _getApplicableLockfilePaths(lockfilePath, lockedPath)

        if remainingLockfiles:
            if len(remainingLockfiles) > 1:
                raise RuntimeError("Multiple lockfiles found for the same path, cannot proceed.")
            remainingLockFile = remainingLockfiles[0]
            remainingPid = remainingLockFile.stem.split("_")[0]
            remainingHost = remainingLockFile.stem.split("_")[1]
            # NOTE: This will not support the application doing many writes in parallel
            if not (remainingPid == str(os.getpid()) and remainingHost == hostName()):
                return False

    return True


def _generateLockfile(lockedPath: Path):
    """Generates or adopts a lockfile for the given lockedPath."""
    # get pid
    lockFileName = _generateLockfileName()
    lockFileRoot = Config["lockfile.root"]
    lockFilePath = Path(f"{lockFileRoot}/{lockFileName}")

    # Ensure the directory exists
    if not lockFilePath.parent.exists():
        lockFilePath.parent.mkdir(parents=True, exist_ok=True)

    maxAgeSeconds = Config["lockfile.ttl"]  # seconds
    checkFrequency = Config["lockfile.checkFrequency"]  # seconds
    timeout = Config["lockfile.timeout"]  # seconds
    while not _reapOldLockfiles(lockFilePath, maxAgeSeconds, lockedPath):
        # if the lockfile still exists, wait for it to be removed
        time.sleep(checkFrequency)
        timeout -= checkFrequency
        if timeout <= 0:
            raise RuntimeError(f"Timeout waiting for lockfile {lockFilePath} to be removed.")

    # Adopt the existing lockfile if it exists
    existingLockfiles = _getApplicableLockfilePaths(lockFilePath, lockedPath)
    if len(existingLockfiles) > 0:
        # if there is an existing lockfile, use that instead
        lockFilePath = existingLockfiles[0]

    # Ensure lockfile is created
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
    lockedPath: Path = None

    def __init__(self, lockedPath: Path):
        super().__init__(lockFilePath=_generateLockfile(lockedPath))
        self.lockedPath = lockedPath.expanduser().resolve()

    def exists(self) -> bool:
        """Check if the lock file exists."""
        return self.lockFilePath is not None and self.lockFilePath.exists()

    def release(self):
        if self.exists():
            # Pop the lockedPath from the lock file
            _removePath(self.lockFilePath, self.lockedPath)
            if not self.lockFilePath.read_text().strip():
                # if the lockfile is empty, remove it
                self.lockFilePath.unlink(missing_ok=True)
            self.lockFilePath = None
            self.lockedPath = None
        else:
            logger.warning("Attempted to release a lock file that does not exist or has already been released.")
