# generate lockfile name
import time
from contextlib import contextmanager
from pathlib import Path

from snapred.meta.Config import Config


def _reapOldLockfiles(lockfilePath, maxAgeSeconds):
    # check if any lockfile exists
    existing_lockfiles = list(lockfilePath.parent.glob("*.lock"))
    if existing_lockfiles:
        # sort by creation time and remove ones older than 10 seconds
        existing_lockfiles = [f for f in existing_lockfiles if time.time() - f.stat().st_ctime > maxAgeSeconds]
        for lockfile in existing_lockfiles:
            lockfile.unlink()

        # if still exists, raise error
        remaining_lockfiles = list(lockfilePath.parent.glob("*.lock"))
        if remaining_lockfiles:
            if len(remaining_lockfiles) > 1:
                raise RuntimeError("Multiple lockfiles found, cannot proceed.")
            remainingLockFile = remaining_lockfiles[0]
            remainingPid = remainingLockFile.name.split("_")[0]
            if remainingPid == str(Path("/proc/self").resolve().name):
                remainingLockFile.unlink()
            else:
                return False
    return True


def _generateLockfile():
    # get pid
    pid = str(Path("/proc/self").resolve().name)
    lockFileRoot = Config["lockfile.root"]
    lockfilePath = Path(f"{lockFileRoot}/{pid}_{time.strftime('%Y%m%d_%H%M%S')}.lock")
    if not lockfilePath.parent.exists():
        lockfilePath.parent.mkdir(parents=True, exist_ok=True)
    # check if any lockfile exists
    maxAgeSeconds = Config["lockfile.ttl"]  # seconds
    timeout = Config["lockfile.timeout"]  # seconds
    while not _reapOldLockfiles(lockfilePath, maxAgeSeconds=maxAgeSeconds):
        # if the lockfile still exists, wait for it to be removed
        time.sleep(maxAgeSeconds)
        timeout -= maxAgeSeconds
        if timeout <= 0:
            raise RuntimeError(f"Timeout waiting for lockfile {lockfilePath} to be removed.")
    # create new lockfile
    lockfilePath.touch(exist_ok=True)
    return lockfilePath


@contextmanager
def lock():
    # Context manager to safely lock a dir for a specific type of operation.

    # __enter__
    # generate a lockfile
    lockFilePath = None
    try:
        lockFilePath = _generateLockfile()
        yield
    finally:
        # __exit__
        if bool(lockFilePath) and lockFilePath.exists():
            lockFilePath.unlink()


class LockFile:
    def __init__(self):
        self.lockFilePath = None
        self.lockFilePath = _generateLockfile()

    def release(self):
        if self.lockFilePath and self.lockFilePath.exists():
            self.lockFilePath.unlink()
            self.lockFilePath = None
        else:
            raise RuntimeError("Lock file was already released or does not exist.")
