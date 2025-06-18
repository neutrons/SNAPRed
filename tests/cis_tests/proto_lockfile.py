# generate lockfile name
import time
from pathlib import Path



def generate_lockfile():
    # get pid
    pid = str(Path("/proc/self").resolve().name)
    
    lockfilePath = Path(f"/tmp/snapred/{pid}_{time.strftime('%Y%m%d_%H%M%S')}.lock")
    if not lockfilePath.parent.exists():
        lockfilePath.parent.mkdir(parents=True, exist_ok=True)
    # check if any lockfile exists
    existing_lockfiles = list(lockfilePath.parent.glob("*.lock"))
    if existing_lockfiles:
        # sort by creation time and remove ones older than 10 seconds
        existing_lockfiles = [f for f in existing_lockfiles if time.time() - f.stat().st_ctime > 10]
        for lockfile in existing_lockfiles:
            lockfile.unlink()
        
        # if still exists, raise error
        remaining_lockfiles = list(lockfilePath.parent.glob("*.lock"))
        if remaining_lockfiles:
            if len(remaining_lockfiles) > 1:
                raise RuntimeError("Multiple lockfiles found, cannot proceed.")
            remainingLockFile = remaining_lockfiles[0]
            remainingPid = remainingLockFile.name.split("_")[0]
            if remainingPid == pid:
                remainingLockFile.unlink()
            else:
                raise RuntimeError(f"Lockfile {remaining_lockfiles[0]} already exists.")
    
    # create new lockfile
    lockfilePath.touch(exist_ok=True)
    return lockfilePath

generate_lockfile()
generate_lockfile()