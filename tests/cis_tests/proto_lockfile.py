from pathlib import Path
from snapred.meta.LockFile import LockFile


lock = LockFile(Path("~"))
lock2 = LockFile(Path("~"))