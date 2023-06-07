from mantid.api import (
    AlgorithmFactory,
    PythonAlgorithm,
    mtd,
)
from mantid.kernel import Direction, FloatTimeSeriesProperty

from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

class SNAPLiteInstrumentCreator(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty(
            "InstrumentFilename", defaultValue="", direction=Direction.Input
        )
        self.declareProperty(
            "DetectorState", defaultValue="", direction=Direction.Input
        )
        self.declareProperty(
            "InputWorkspace", defaultValue="", direction=Direction.Input
        )
        self.declareProperty(
            "OutputWorkspace", defaultValue="", direction=Direction.Output
        )
        self.mantidSnapper = MantidSnapper(self, "SNAPLiteInstrumentCreator")
        self.setRethrows(True)

    def PyExec(self):
        self.log().notice("Create SNAP Lite instrument based on the Instrument Definition File")

        idf = self.getProperty("InstrumentDefinitionFile")
        detectorState = self.getProperty("DetectorState") 
        inputWorkspacePath = self.getProperty("InputWorkspace").value
        
        # load the idf into workspace
        ei_ws = self.mantidSnapper.LoadEmptyInstrument("Loading empty instrument...",
                                                    FileName=idf,
                                                    OutputWorkspace=inputWorkspacePath)
        self.mantidSnapper.executeQueue()
        ei_ws = mtd[ei_ws]
        
        # create sample logs
        zeroTime = "1990-01-01T00:00:00" # GPS epoch

        ang1Log = FloatTimeSeriesProperty("det_arc1")
        ang1Log.addValue(zeroTime, detectorState.arc1.minimum)

        ang2Log = FloatTimeSeriesProperty("det_arc2")
        ang2Log.addValue(zeroTime, detectorState.det_arc2.maximum)

        len1Log = FloatTimeSeriesProperty("det_lin1")
        len1Log.addValue(zeroTime, detectorState.det_lin1.minimum)

        len2Log = FloatTimeSeriesProperty("det_lin2")
        len2Log.addValue(zeroTime, detectorState.det_lin2.maximum)
 
        # add sample logs to the workspace
        for log in [ang1Log,ang2Log,len1Log,len2Log]:
            ei_ws.addProperty(log)

        # reload instrument so the logs are used
        ei_with_logs_ws = self.mantidSnapper.LoadInstrument("Loading empty instrument...",
                                                    Workspace=inputWorkspacePath,
                                                    FileName=idf,
                                                    MonitorList="-2--1",
                                                    RewriteSpectraMap="False")
        self.mantidSnapper.executeQueue()
        ei_with_logs_ws = mtd[ei_with_logs_ws]

        # set the units so DiffractionFocussing will do its job
        ei_with_logs_ws.getAxis(0).setUnit("dSpacing")
 
# Register algorithm with Mantid
AlgorithmFactory.subscribe(SNAPLiteInstrumentCreator)
