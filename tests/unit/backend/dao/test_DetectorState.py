# note: this runs the same checks as the calibrant_samples_script CIS test
import unittest

from snapred.backend.dao.state.DetectorState import DetectorState


class TestDetectorstate(unittest.TestCase):
    def setUp(self):
        self.logs = {
            "det_lin1": (1.0,),
            "det_lin2": (1.1,),
            "det_arc1": (2.2,),
            "det_arc2": (2.3,),
            "BL3:Chop:Skf1:WavelengthUserReq": (3.4,),
            "BL3:Det:TH:BL:Frequency": (4.5,),
            "BL3:Mot:OpticsPos:Pos": (2,),
        }
        self.detectorState = DetectorState(
            lin=(self.logs["det_lin1"][0], self.logs["det_lin2"][0]),
            arc=(self.logs["det_arc1"][0], self.logs["det_arc2"][0]),
            wav=self.logs["BL3:Chop:Skf1:WavelengthUserReq"][0],
            freq=self.logs["BL3:Det:TH:BL:Frequency"][0],
            guideStat=self.logs["BL3:Mot:OpticsPos:Pos"][0],
        )

    def test_fromLogs(self):
        detectorState = DetectorState.fromLogs(self.logs)
        assert detectorState.arc[0] == self.logs["det_arc1"][0]
        assert detectorState.arc[1] == self.logs["det_arc2"][0]
        assert detectorState.lin[0] == self.logs["det_lin1"][0]
        assert detectorState.lin[1] == self.logs["det_lin2"][0]
        assert detectorState.wav == self.logs["BL3:Chop:Skf1:WavelengthUserReq"][0]
        assert detectorState.freq == self.logs["BL3:Det:TH:BL:Frequency"][0]
        assert detectorState.guideStat == self.logs["BL3:Mot:OpticsPos:Pos"][0]

    def test_toLogs(self):
        assert self.logs == self.detectorState.toLogs()
