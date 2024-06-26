# ruff: noqa: PT011

from random import randint

import pytest
from numpy import int64
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.indexing.Versioning import (
    VERSION_DEFAULT,
    VERSION_DEFAULT_NAME,
    VERSION_NONE_NAME,
    VersionedObject,
)


def test_init_bad():
    with pytest.raises(ValueError):
        VersionedObject(version="bad")
    with pytest.raises(ValueError):
        VersionedObject(version=-2)
    with pytest.raises(ValueError):
        VersionedObject(version=1.2)


def test_init_name_none():
    vo = VersionedObject(version=VERSION_NONE_NAME)
    assert vo.version is None


def test_init_name_default():
    vo = VersionedObject(version=VERSION_DEFAULT_NAME)
    assert vo.version == VERSION_DEFAULT


def test_init_none():
    vo = VersionedObject(version=None)
    assert vo.version is None


def test_init_default():
    vo = VersionedObject(version=VERSION_DEFAULT)
    assert vo.version == VERSION_DEFAULT


def test_init_int():
    for i in range(10):
        version = randint(0, 1000)
        vo = VersionedObject(version=version)
        assert vo.version == version


def test_init_int64():
    """
    The NexusHDF5Metadata service reads in integers as numpy.int64
    Make sure this can vbe validly interpretted as an integer.
    """
    for i in range(10):
        version = int64(randint(0, 1000))
        assert not isinstance(version, int)
        vo = VersionedObject(version=version)
        assert vo.version == version


def test_write_version_int():
    for i in range(10):
        version = randint(0, 1000)
        vo = VersionedObject(version=version)
        assert vo.model_dump_json() == f'{{"version":{version}}}'
        assert vo.dict()["version"] == version


def test_write_version_none():
    vo = VersionedObject(version=None)
    assert vo.version is None
    assert vo.model_dump_json() == f'{{"version":"{VERSION_NONE_NAME}"}}'
    assert vo.model_dump_json() != '{"version":null}'
    assert vo.dict()["version"] == VERSION_NONE_NAME


def test_write_version_default():
    vo = VersionedObject(version=VERSION_DEFAULT_NAME)
    assert vo.version == VERSION_DEFAULT
    assert vo.model_dump_json() == f'{{"version":"{VERSION_DEFAULT_NAME}"}}'
    assert vo.model_dump_json() != f'{{"version":{VERSION_DEFAULT}}}'
    assert vo.dict()["version"] == VERSION_DEFAULT_NAME


def test_can_set_valid():
    old_version = randint(0, 120)
    new_version = randint(0, 120)
    vo = VersionedObject(version=old_version)
    vo.version = new_version
    assert vo.version == new_version

    vo.version = VERSION_DEFAULT
    assert vo.version == VERSION_DEFAULT

    vo.version = VERSION_DEFAULT_NAME
    assert vo.version == VERSION_DEFAULT


def test_cannot_set_invalid():
    vo = VersionedObject(version=randint(0, 120))
    with pytest.raises(ValueError):
        vo.version = None
    with pytest.raises(ValueError):
        vo.version = "matt"
    with pytest.raises(ValueError):
        vo.version = -2


def test_shaped_liked_itself():
    """
    Make a versioned object.  Serialize it.  Then parse it back as itself.
    It should validate and create an identical object.
    """
    # version is None
    vo_old = VersionedObject(version=None)
    vo_new = VersionedObject.model_validate(vo_old.model_dump())
    assert vo_old == vo_new
    vo_new = VersionedObject.model_validate(vo_old.dict())
    assert vo_old == vo_new
    vo_new = VersionedObject.model_validate_json(vo_old.model_dump_json())
    assert vo_old == vo_new

    # version is default
    vo_old = VersionedObject(version=VERSION_DEFAULT)
    vo_new = VersionedObject.model_validate(vo_old.model_dump())
    assert vo_old == vo_new
    vo_new = VersionedObject.model_validate(vo_old.dict())
    assert vo_old == vo_new
    vo_new = VersionedObject.model_validate_json(vo_old.model_dump_json())
    assert vo_old == vo_new

    # version is integer
    vo_old = VersionedObject(version=randint(0, 120))
    vo_new = VersionedObject.model_validate(vo_old.model_dump())
    assert vo_old == vo_new
    vo_new = VersionedObject.model_validate(vo_old.dict())
    assert vo_old == vo_new
    vo_new = VersionedObject.model_validate_json(vo_old.model_dump_json())
    assert vo_old == vo_new


### TESTS OF INDEX ENTRIES AS VERSIONED OBJECTS ###


def indexEntryWithVersion(version):
    return IndexEntry(
        version=version,
        runNumber="xyz",
        useLiteMode=True,
    )


def test_init_bad_index_entry():
    with pytest.raises(ValueError):
        indexEntryWithVersion("bad")
    with pytest.raises(ValueError):
        indexEntryWithVersion(-2)
    with pytest.raises(ValueError):
        indexEntryWithVersion(1.2)


def test_init_index_entry():
    # init from valid integers
    for i in range(10):
        version = randint(0, 1000)
        vo = indexEntryWithVersion(version)
        assert vo.version == version

    # init with none
    vo = indexEntryWithVersion(VERSION_NONE_NAME)
    assert vo.version is None
    vo = indexEntryWithVersion(None)
    assert vo.version is None

    # init with default
    vo = indexEntryWithVersion(VERSION_DEFAULT_NAME)
    assert vo.version == VERSION_DEFAULT
    vo = indexEntryWithVersion(VERSION_DEFAULT)
    assert vo.version == VERSION_DEFAULT


def test_set_index_entry():
    old_version = randint(0, 120)
    new_version = randint(0, 120)
    vo = indexEntryWithVersion(old_version)
    vo.version = new_version
    assert vo.version == new_version

    vo.version = VERSION_DEFAULT
    assert vo.version == VERSION_DEFAULT

    vo.version = VERSION_DEFAULT_NAME
    assert vo.version == VERSION_DEFAULT

    vo = indexEntryWithVersion(randint(0, 120))
    with pytest.raises(ValueError):
        vo.version = None
    with pytest.raises(ValueError):
        vo.version = "matt"
    with pytest.raises(ValueError):
        vo.version = -2


def test_write_version_index_entry():
    # test write valid integer
    for i in range(10):
        version = randint(0, 1000)
        vo = indexEntryWithVersion(version)
        assert f'"version":{version}' in vo.model_dump_json()
        assert vo.dict()["version"] == version

    # test write none
    vo = indexEntryWithVersion(None)
    assert vo.version is None
    assert f'"version":"{VERSION_NONE_NAME}"' in vo.model_dump_json()
    assert '"version":null' not in vo.model_dump_json()
    assert vo.dict()["version"] == VERSION_NONE_NAME

    # test write default
    vo = indexEntryWithVersion(VERSION_DEFAULT)
    assert vo.version == VERSION_DEFAULT
    assert f'"version":"{VERSION_DEFAULT_NAME}"' in vo.model_dump_json()
    assert f'"version":{VERSION_DEFAULT}' not in vo.model_dump_json()
    assert vo.dict()["version"] == VERSION_DEFAULT_NAME


### TESTS OF RECORDS AS VERSIONED OBJECTS ###


def recordWithVersion(version):
    return Record(
        version=version,
        runNumber="xyz",
        useLiteMode=True,
        calculationParameters=CalculationParameters.construct(),
    )


def test_init_bad_record():
    with pytest.raises(ValueError):
        recordWithVersion("bad")
    with pytest.raises(ValueError):
        recordWithVersion(-2)
    with pytest.raises(ValueError):
        recordWithVersion(1.2)


def test_init_record():
    # init from valid integers
    for i in range(10):
        version = randint(0, 1000)
        vo = recordWithVersion(version)
        assert vo.version == version

    # init with none
    vo = recordWithVersion(VERSION_NONE_NAME)
    assert vo.version is None
    vo = recordWithVersion(None)
    assert vo.version is None

    # init with default
    vo = recordWithVersion(VERSION_DEFAULT_NAME)
    assert vo.version == VERSION_DEFAULT
    vo = recordWithVersion(VERSION_DEFAULT)
    assert vo.version == VERSION_DEFAULT


def test_set_record():
    old_version = randint(0, 120)
    new_version = randint(0, 120)
    vo = recordWithVersion(old_version)
    vo.version = new_version
    assert vo.version == new_version

    vo.version = VERSION_DEFAULT
    assert vo.version == VERSION_DEFAULT

    vo.version = VERSION_DEFAULT_NAME
    assert vo.version == VERSION_DEFAULT

    vo = recordWithVersion(randint(0, 120))
    with pytest.raises(ValueError):
        vo.version = None
    with pytest.raises(ValueError):
        vo.version = "matt"
    with pytest.raises(ValueError):
        vo.version = -2


def test_write_version_record():
    # test write valid integer
    for i in range(10):
        version = randint(0, 1000)
        vo = recordWithVersion(version)
        assert f'"version":{version}' in vo.model_dump_json()
        assert vo.dict()["version"] == version

    # test write none
    vo = recordWithVersion(None)
    assert vo.version is None
    assert f'"version":"{VERSION_NONE_NAME}"' in vo.model_dump_json()
    assert '"version":null' not in vo.model_dump_json()
    assert vo.dict()["version"] == VERSION_NONE_NAME

    # test write default
    vo = recordWithVersion(VERSION_DEFAULT)
    assert vo.version == VERSION_DEFAULT
    assert f'"version":"{VERSION_DEFAULT_NAME}"' in vo.model_dump_json()
    assert f'"version":{VERSION_DEFAULT}' not in vo.model_dump_json()
    assert vo.dict()["version"] == VERSION_DEFAULT_NAME
