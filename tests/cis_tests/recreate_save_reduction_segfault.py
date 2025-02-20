from snapred.backend.dao.reduction.ReductionRecord import ReductionRecord
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.Utensils import Utensils
import h5py
from snapred.backend.data.NexusHDF5Metadata import NexusHDF5Metadata as n5m
from snapred.meta.redantic import parse_file_as
import time

inputFile = "/SNS/users/wqp/SNAP/IPTS-24641/shared/SNAPRed/04bd2c53f6bf6754/lite/46680/2024-11-26T121144/reduced_046680_2024-11-26T121144.nxs.h5"
outputFile = "/SNS/users/wqp/tmp/test_segfault_append.nxs"
record = parse_file_as(ReductionRecord, "/SNS/users/wqp/SNAP/IPTS-24641/shared/SNAPRed/04bd2c53f6bf6754/lite/46680/2024-11-26T121144/ReductionRecord.json")

untensils = Utensils()
untensils.PyInit()
mantidSnapper = untensils.mantidSnapper

ws = mantidSnapper.LoadNexus("..", Filename=inputFile, OutputWorkspace="ws")
mantidSnapper.executeQueue()

with open("snapred_script.log", "w") as f:
    import faulthandler
    faulthandler.enable(file=f)
    try:
        for i in range(100):
            for wsName in mantidSnapper.mtd[ws].getNames():
                mantidSnapper.SaveNexus("..", InputWorkspace=wsName, Filename=outputFile, Append=True)
                mantidSnapper.executeQueue()

            # commented out to see if this somehow competed with SaveNexus, it does not
            # with h5py.File(outputFile, "a") as h5:
            #         n5m.insertMetadataGroup(h5, record.dict(), "/metadata")

            import os
            os.remove(outputFile)
    finally:
        faulthandler.disable()

    
    