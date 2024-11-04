import os
import tempfile
from pathlib import Path
from typing import List

from mantid.kernel import ConfigService
from mantid.simpleapi import GetIPTS
from util.IPTS_override import IPTS_override

from snapred.meta.Config import Config, Resource


def touch(path):
    with open(path, "a"):
        os.utime(path, None)


def prepareDirectories(tmpDir: str) -> List[str]:
    # Create a bunch of IPTS directories, with a few sets of empty run data:
    searchDirectories = [
        Path(tmpDir) / "SNAP" / "IPTS-24641" / "nexus",
        Path(tmpDir) / "SNAP" / "IPTS-24913" / "nexus",
    ]
    runData = [
        Path(tmpDir) / "SNAP" / "IPTS-24641" / "nexus" / "SNAP_46680.nxs.h5",
        Path(tmpDir) / "SNAP" / "IPTS-24913" / "nexus" / "SNAP_58813.nxs.h5",
        Path(tmpDir) / "SNAP" / "IPTS-24913" / "nexus" / "SNAP_58882.nxs.h5",
    ]
    for dir_ in searchDirectories:
        os.makedirs(dir_)
    for run in runData:
        touch(run)
    return searchDirectories


def test_IPTS_override_amends_config():
    DATASEARCH_DIR = "datasearch.directories"

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        searchDirectories = prepareDirectories(tmpDir)
        config = ConfigService.Instance()
        with IPTS_override(Path(tmpDir), "SNAP") as IPTS_root:  # noqa: F841
            configSearchDirs = config[DATASEARCH_DIR]
            for dir_ in searchDirectories:
                assert str(dir_) in configSearchDirs


def test_IPTS_override_amends_SNAPRed_config():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        prepareDirectories(tmpDir)
        with IPTS_override(Path(tmpDir), "SNAP") as IPTS_root:
            assert Config["IPTS.root"] == IPTS_root


def test_IPTS_override_exit_Mantid_config():
    DATASEARCH_DIR = "datasearch.directories"

    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        # Create a bunch of IPTS directories, with a few sets of empty run data:
        searchDirectories = prepareDirectories(tmpDir)
        config = ConfigService.Instance()
        with IPTS_override(Path(tmpDir), "SNAP") as IPTS_root:  # noqa: F841
            configSearchDirs = config[DATASEARCH_DIR]
            for dir_ in searchDirectories:
                assert str(dir_) in configSearchDirs
        configSearchDirs = config[DATASEARCH_DIR]
        for dir_ in searchDirectories:
            assert str(dir_) not in configSearchDirs


def test_IPTS_override_exit_SNAPRed_config():
    IPTS_root_save = Config["IPTS.root"]
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        # Create a bunch of IPTS directories, with a few sets of empty run data:
        prepareDirectories(tmpDir)
        with IPTS_override(Path(tmpDir), "SNAP") as IPTS_root:
            assert Config["IPTS.root"] == IPTS_root
        assert Config["IPTS.root"] == IPTS_root_save


def test_IPTS_override_overrides_single():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        prepareDirectories(tmpDir)
        with IPTS_override(Path(tmpDir), "SNAP") as IPTS_root:  # noqa: F841
            assert Path(GetIPTS("46680", "SNAP")) == Path(tmpDir) / "SNAP" / "IPTS-24641"


def test_IPTS_override_overrides_multiple():
    with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpDir:
        prepareDirectories(tmpDir)
        with IPTS_override(Path(tmpDir), "SNAP") as IPTS_root:  # noqa: F841
            assert Path(GetIPTS("58813", "SNAP")) == Path(GetIPTS("58882", "SNAP"))
