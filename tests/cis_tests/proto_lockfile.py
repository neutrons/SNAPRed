from pathlib import Path
from snapred.meta.LockFile import LockFile


lock = LockFile(Path("~"))
print(lock.lockFilePath)
print(lock.lockFilePath.read_text())
lock2 = LockFile(Path("~"))
print(lock2.lockFilePath)
print(lock2.lockFilePath.read_text())
lock2.release()
print("release lock2")
print(lock.lockFilePath.read_text())
print("release lock1")
lock.release()
print("Does lock file still exist?", lock.exists())