import multiprocessing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pytest
from util.Config_helpers import Config_override

from snapred.meta.LockFile import LockFile, LockManager


def create_lock_file(lockedPath_str, timeout=10, ttl=1):
    with Config_override("lockfile.timeout", timeout), Config_override("lockfile.ttl", ttl):
        lockedPath = Path(lockedPath_str)
        lockFile = LockFile(lockedPath)
        assert lockFile.exists()
        # Return the lock file path as string, since LockFile objects can't be pickled
        return str(lockFile.lockFilePath)


class TestLockFile:
    @pytest.fixture(autouse=True, scope="function")  # noqa: PT003
    @classmethod
    def _setup_test_data(cls):
        # setup temp directory to save lock files to
        cls.temp_dir = TemporaryDirectory()
        with Config_override("lockfile.root", cls.temp_dir.name):
            try:
                yield
            finally:
                # Cleanup the temporary directory after tests
                cls.temp_dir.cleanup()
                cls.temp_dir = None

    def test_lockFileCreated(self):
        with TemporaryDirectory() as temp_dir:
            lockFile = LockFile(Path(temp_dir))
            assert lockFile.lockFilePath.exists()
            assert lockFile.lockFilePath.is_file()
            assert lockFile.lockFilePath.name.endswith(".lock")
            assert temp_dir in str(lockFile.lockFilePath.read_text())

    def test_lockFileRelease(self):
        with TemporaryDirectory() as temp_dir:
            lockFile = LockFile(Path(temp_dir))
            assert lockFile.exists()
            lockFile.release()
            assert not lockFile.exists()

    def test_lockFileContextManager(self):
        with TemporaryDirectory() as temp_dir:
            lockedPath = Path(temp_dir)
            with LockManager(lockedPath) as lockFile:
                assert lockFile.exists()
            assert not lockFile.exists()

    def test_lockFileMultipleInSameProcess(self):
        with TemporaryDirectory() as temp_dir:
            lockedPath = Path(temp_dir)
            lockFile1 = LockFile(lockedPath)
            lockFile2 = LockFile(lockedPath)
            # new lockfile should be created
            assert lockFile1.lockFilePath == lockFile2.lockFilePath
            assert lockFile1.exists()
            assert lockFile2.exists()

            lockFile2.release()
            assert lockFile1.exists()  # lockFile1 should still exist
            assert not lockFile2.exists()  # Both should be released

    def test_lockFileInDifferentProcesses(self):
        with TemporaryDirectory() as temp_dir:
            lockedPath = Path(temp_dir)
            lockedPath_str = str(lockedPath)
            lockFilePaths = []
            with (Config_override("lockfile.ttl", 0), 
                  Config_override("lockfile.checkFrequency", 0.1), 
                  Config_override("lockfile.timeout", 1)):
                with multiprocessing.Pool(processes=1) as pool:
                    lockFilePaths.extend(pool.map(create_lock_file, [lockedPath_str]))
                    
                with multiprocessing.Pool(processes=1) as pool:
                    lockFilePaths.extend(pool.map(create_lock_file, [lockedPath_str]))

            assert len(lockFilePaths) == 2
            assert lockFilePaths[0] != lockFilePaths[1]

            pid1 = Path(lockFilePaths[0]).name.split("_")[0]
            pid2 = Path(lockFilePaths[1]).name.split("_")[0]
            assert pid1 != pid2  # Different processes should have different PIDs

    def test_lockInDifferentProcesses_timeout(self):
        with TemporaryDirectory() as temp_dir:
            lockedPath = Path(temp_dir)
            lockedPath_str = str(lockedPath)

            # Create a lock file in the main process
            lockFile = LockFile(lockedPath)
            assert lockFile.exists()

            # Try to create a lock file in another process, which should timeout
            with multiprocessing.Pool(processes=1) as pool:
                with pytest.raises(RuntimeError, match="Timeout waiting for lockfile"):
                    # This will attempt to create a lock file in a separate process
                    # but should fail due to the existing lock file
                    pool.apply(
                        create_lock_file,
                        (lockedPath_str, 0, 0.1),
                    )

            # Clean up the lock file
            lockFile.release()

    def test_lockFileDifferentPaths(self):
        with TemporaryDirectory() as temp_dir1, TemporaryDirectory() as temp_dir2:
            lockedPath1 = Path(temp_dir1)
            lockedPath2 = Path(temp_dir2)

            lockFile1 = LockFile(lockedPath1)
            lockFile2 = LockFile(lockedPath2)

            assert lockFile1.lockFilePath == lockFile2.lockFilePath
            assert lockFile1.exists()
            assert lockFile2.exists()

            lockFile1.release()
            lockFile2.release()

            assert not lockFile1.exists()
            assert not lockFile2.exists()

    def test_reapOldLockfiles(self):
        with TemporaryDirectory() as temp_dir:
            lockedPath = Path(temp_dir)
            for i in range(3):
                lockFile = LockFile(lockedPath / f"test_lock_{i}")
                assert lockFile.exists()

            with Config_override("lockfile.ttl", 0):
                assert len(list(lockFile.lockFilePath.parent.glob("*.lock"))) == 1, (
                    f"contents of dir: {list(lockFile.lockFilePath.parent.glob('*'))}"
                )
                assert len(lockFile.lockFilePath.read_text().splitlines()) == 3, (
                    f"contents of lock file: {lockFile.lockFilePath.read_text()}"
                )
                # Reap old lockfiles
                lock = LockFile(lockedPath)
                assert len(list(lockFile.lockFilePath.parent.glob("*.lock"))) == 1
                assert len(lock.lockFilePath.read_text().splitlines()) == 1, (
                    f"contents of lock file: {lock.lockFilePath.read_text()}"
                )

    def test_doubleRelease(self):
        with TemporaryDirectory() as temp_dir:
            lockedPath = Path(temp_dir)
            lockFile = LockFile(lockedPath)
            with mock.patch("snapred.meta.LockFile.logger") as mockLogger:
                assert lockFile.exists()
                lockFile.release()
                assert not lockFile.exists()

                # Releasing again should not raise an error
                lockFile.release()
                assert not lockFile.exists()
                assert mockLogger.warning.call_count == 1
                assert "has already been released." in mockLogger.warning.call_args[0][0]
