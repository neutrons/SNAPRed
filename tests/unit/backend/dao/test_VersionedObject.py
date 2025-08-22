# ruff: noqa: PT011

from random import randint

import pytest
from numpy import int64
from util.dao import DAOFactory

from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.indexing.VersionedObject import VersionedObject
from snapred.backend.dao.indexing.Versioning import (
    VERSION_START,
    VersionState,
)


def test_init_bad():
    with pytest.raises(ValueError):
        VersionedObject(version="bad")
    with pytest.raises(ValueError):
        VersionedObject(version=-2)
    with pytest.raises(ValueError):
        VersionedObject(version=1.2)


def test_init_name_default():
    vo = VersionedObject(version=VersionState.DEFAULT, indexEntry=DAOFactory.indexEntry())
    assert vo.version == VersionState.DEFAULT


def test_init_none():
    with pytest.raises(ValueError):
        VersionedObject(version=None, indexEntry=DAOFactory.indexEntry())


def test_init_default():
    vo = VersionedObject(version=VersionState.DEFAULT, indexEntry=DAOFactory.indexEntry())
    assert vo.version == VersionState.DEFAULT


def test_init_int():
    for i in range(10):
        version = randint(0, 1000)
        vo = VersionedObject(version=version, indexEntry=DAOFactory.indexEntry())
        assert vo.version == version


def test_init_int64():
    """
    The NexusHDF5Metadata service reads in integers as numpy.int64
    Make sure this can vbe validly interpretted as an integer.
    """
    for i in range(10):
        version = int64(randint(0, 1000))
        assert not isinstance(version, int)
        vo = VersionedObject(version=version, indexEntry=DAOFactory.indexEntry())
        assert vo.version == version


def test_write_version_int():
    for i in range(10):
        version = randint(0, 1000)
        vo = VersionedObject(version=version, indexEntry=DAOFactory.indexEntry())
        assert vo.model_dump_json() == f'{{"version":{version}}}'
        assert vo.model_dump()["version"] == version


def test_write_version_none():
    with pytest.raises(ValueError):
        VersionedObject(version=None, indexEntry=DAOFactory.indexEntry())


def test_write_version_default():
    vo = VersionedObject(version=VERSION_START(), indexEntry=DAOFactory.indexEntry())
    assert vo.version == VERSION_START()
    assert vo.model_dump_json() == f'{{"version":{VERSION_START()}}}'
    assert vo.model_dump_json() != f'{{"version":"{VERSION_START()}"}}'
    assert vo.model_dump()["version"] == VERSION_START()


def test_can_set_valid():
    old_version = randint(0, 120)
    new_version = randint(0, 120)
    vo = VersionedObject(version=old_version, indexEntry=DAOFactory.indexEntry())
    vo.version = new_version
    assert vo.version == new_version

    vo.version = VersionState.DEFAULT
    assert vo.version == VersionState.DEFAULT

    vo.version = VersionState.DEFAULT
    assert vo.version == VersionState.DEFAULT


def test_cannot_set_invalid():
    vo = VersionedObject(version=randint(0, 120), indexEntry=DAOFactory.indexEntry())
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

    # version is default
    vo_old = VersionedObject(version=VersionState.DEFAULT, indexEntry=DAOFactory.indexEntry())
    with pytest.raises(ValueError, match="must be flattened to an int before writing to"):
        vo_old.model_dump_json()
    vo_old.version = VERSION_START()
    vo_new = VersionedObject.model_validate(vo_old.model_dump())
    assert vo_old == vo_new
    vo_new = VersionedObject.model_validate(vo_old.model_dump())
    assert vo_old == vo_new
    vo_new = VersionedObject.model_validate_json(vo_old.model_dump_json())
    assert vo_old == vo_new

    # version is integer
    vo_old = VersionedObject(version=randint(0, 120), indexEntry=DAOFactory.indexEntry())
    vo_new = VersionedObject.model_validate(vo_old.model_dump())
    assert vo_old == vo_new
    vo_new = VersionedObject.model_validate(vo_old.model_dump())
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

    # init with default
    vo = indexEntryWithVersion(VersionState.DEFAULT)
    # This must be flattened to an int before writing to JSON
    assert vo.version == VersionState.DEFAULT


def test_set_index_entry():
    old_version = randint(0, 120)
    new_version = randint(0, 120)
    vo = indexEntryWithVersion(old_version)
    vo.version = new_version
    assert vo.version == new_version

    vo.version = VERSION_START()
    assert vo.version == VERSION_START()

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

    # test write default
    vo = indexEntryWithVersion(VersionState.DEFAULT)

    with pytest.raises(ValueError, match="must be flattened to an int before writing to"):
        vo.model_dump_json()

    vo.version = VERSION_START()
    assert f'"version":{VERSION_START()}' in vo.model_dump_json()
    assert f'"version":"{VERSION_START()}"' not in vo.model_dump_json()
    assert vo.model_dump()["version"] == VERSION_START()


### TESTS OF RECORDS AS VERSIONED OBJECTS ###


def recordWithVersion(version):
    return Record(
        version=version,
        runNumber="xyz",
        useLiteMode=True,
        calculationParameters=CalculationParameters.model_construct(),
        indexEntry=DAOFactory.indexEntry(),
        snapredVersion="test",
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

    # init with default
    vo = recordWithVersion(VersionState.DEFAULT)
    assert vo.version == VersionState.DEFAULT
    vo = recordWithVersion(VersionState.DEFAULT)
    assert vo.version == VersionState.DEFAULT


def test_init_record_no_snapred_version():
    record = recordWithVersion(1)
    record.snapredVersion = "unknown"
    with pytest.raises(ValueError, match="snapredVersion is required for Record serialization."):
        record.model_dump_json()
    record.snapredVersion = "test"
    record.model_dump_json()  # Should not raise an error now


def test_set_record():
    old_version = randint(0, 120)
    new_version = randint(0, 120)
    vo = recordWithVersion(old_version)
    vo.version = new_version
    assert vo.version == new_version

    vo.version = VersionState.DEFAULT
    assert vo.version == VersionState.DEFAULT

    vo.version = VersionState.DEFAULT
    assert vo.version == VersionState.DEFAULT

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
        assert vo.model_dump()["version"] == version

    # test write default
    vo = recordWithVersion(VersionState.DEFAULT)
    assert vo.version == VersionState.DEFAULT

    with pytest.raises(ValueError, match="must be flattened to an int before writing to"):
        vo.model_dump_json()
    vo.version = VERSION_START()

    assert f'"version":{VERSION_START()}' in vo.model_dump_json()
    assert f'"version":"{VERSION_START()}"' not in vo.model_dump_json()
    assert vo.model_dump()["version"] == VERSION_START()
