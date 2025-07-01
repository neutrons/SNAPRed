

import json
import shutil
from pathlib import Path
import time
from typing import List

from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.Config import Config
from snapred.meta.redantic import parse_file_as


calibrationRoot = Path(Config["instrument.calibration.home"])
calibrationStateRoot = Path(Config["instrument.calibration.powder.home"])
stageDir = calibrationRoot / "staged_migration"

if stageDir.exists():
        raise ValueError(f"Stage directory {stageDir} already exists, please remove it before running the migration.")
stageDir.mkdir(exist_ok=False)
# 1. walk all subfolders, ones that contain CalibrationIndex.json or NormalizationIndex.json must be migrated, collect them to a list. 
# this must be done recursively
lds = LocalDataService()
instParamIndexer = lds.instrumentParameterIndexer()


def findIndexDirs(root: Path) -> list[Path]:
    indexDirs = []
    additionalSearchDirs = []
    for path in root.rglob("*"):
        if path.is_dir():
            if (path / "CalibrationIndex.json").exists() or (path / "NormalizationIndex.json").exists():
                indexDirs.append(path)
            else:
                # If the directory does not contain the index files, we can still add it to the search list
                additionalSearchDirs.append(path)
    subDirIndexes = [findIndexDirs(subdir) for subdir in additionalSearchDirs]
    subDirIndexes = [d for d in subDirIndexes if len(d) > 0] # filter out empty lists
    for indexDir in subDirIndexes:
        indexDirs.extend(indexDir)  # Flatten the list of lists
    # filter out duplicates
    indexDirs = list(set(indexDirs))
    return indexDirs

indexDirs = findIndexDirs(calibrationStateRoot)
indexDirs.append(instParamIndexer.rootDirectory)
print("\n".join([str(p) for p in indexDirs]))
# take cmd input asking if the above list of directories is correct
confirm = input("Migrate the above Directories? (y/n): ").strip().lower()
if confirm != 'y':
    print("Exiting without making any changes.")
    exit(0)
    

def stageMigrationIndexDir(indexDir: Path):
    
    indexFile = indexDir
    # create temp adjacent directory to stage the changes before moving them

    if (indexFile / "CalibrationIndex.json").exists():
        indexFile = indexFile / "CalibrationIndex.json"
    elif (indexFile / "NormalizationIndex.json").exists():
        indexFile = indexFile / "NormalizationIndex.json"
    elif (indexFile / "InstrumentParameterIndex.json").exists():
        indexFile = indexFile / "InstrumentParameterIndex.json"
    else:
        raise ValueError(f"Index file not found in {indexDir}")
    print(f"Migrating {indexFile}")
    
    # read the index file
    indexList = parse_file_as(List[IndexEntry], indexFile)
    # for each entry, alter the jsons of the referenced version folders to contain the indexEntry
    for entry in indexList:
        folderPath = indexDir / wnvf.pathVersion(entry.version)
        if not folderPath.exists():
            print(f"Folder {folderPath} does not exist, skipping entry {entry}")
            continue
        jsonFiles = list(folderPath.glob("*.json"))
        if not jsonFiles:
            print(f"No JSON files found in {folderPath}, skipping entry {entry}")
            continue
        for jsonFile in jsonFiles:
            jDict = None
            print(f"Adding indexEntry to {jsonFile}")
            # read in json as dict
            with open(jsonFile, 'r') as f:
                jDict = json.load(f)
            # add the indexEntry to the dict
            jDict["indexEntry"] = entry.model_dump()
            
            # if the file is a "record" we also need to update its "calculationParameters" to match the version of the indexEntry
            if "calculationParameters" in jDict:
                jDict["calculationParameters"]["indexEntry"] = entry.model_dump()
                jDict["calculationParameters"]["instrumentState"]["instrumentConfig"]["indexEntry"] = instParamIndexer.latestApplicableEntry(jDict["runNumber"]).model_dump()
            if "instrumentState" in jDict:
                jDict["instrumentState"]["instrumentConfig"]["indexEntry"] = instParamIndexer.latestApplicableEntry(jDict["seedRun"]).model_dump()
            
            # write the dict back to the json file
            # find common root of stageDir and jsonFile
            stagedJsonFile = stageDir / jsonFile.relative_to(calibrationRoot)
            stagedJsonFile.parent.mkdir(parents=True, exist_ok=True)
            with open(stagedJsonFile, 'w') as f:
                json.dump(jDict, f, indent=4)
    print(f"Migration of {indexFile} staged.")
    # prompt user to confirm the changes look correct
    
for d in indexDirs:
    stageMigrationIndexDir(d)
    
confirm = input(f"Do the staged changes ({str(stageDir)}) look correct?  Original files will be backed-up. (y/n): ").strip().lower()
if confirm != 'y':
    print("Removing staged changes.")
    
    shutil.rmtree(stageDir)
    exit(0)
else:
    # migrate...
    # backup the original files
    backupDir = calibrationRoot.parent / f"backup_migration{time.strftime('%Y%m%d_%H%M%S')}"
    print(f"Backing up original files to {backupDir}")
    backupDir.mkdir(exist_ok=True)
    
    # for each file in stageDir, move the original file to the backupDir and then move the staged file to the original location
    for stagedFile in stageDir.rglob("*"):
        if stagedFile.is_file():
            # find the original file
            originalFile = calibrationRoot / stagedFile.relative_to(stageDir)
            if originalFile.exists():
                # move the original file to the backup directory
                backupFile = backupDir / originalFile.relative_to(calibrationRoot)
                backupFile.parent.mkdir(parents=True, exist_ok=True)
                originalFile.rename(backupFile)
            # move the staged file to the original location
            stagedFile.rename(originalFile)
    print("Migration complete. Original files backed up.")
# remove the stage directory
shutil.rmtree(stageDir)
print("Stage directory removed.")