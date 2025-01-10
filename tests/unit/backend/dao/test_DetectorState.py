# note: this runs the same checks as the calibrant_samples_script CIS test
import unittest

from snapred.backend.dao.state.DetectorState import DetectorState


class TestDetectorstate(unittest.TestCase):
    def test_getLogValues(self):
        exp = {
            "det_lin1": "1.0",
            "det_lin2": "1.1",
            "det_arc1": "2.2",
            "det_arc2": "2.3",
            "BL3:Chop:Skf1:WavelengthUserReq": "3.4",
            "BL3:Det:TH:BL:Frequency": "4.5",
            "BL3:Mot:OpticsPos:Pos": "2",
        }
        detectorState = DetectorState.constructFromLogValues(exp)
        assert detectorState.arc[0] == float(exp["det_arc1"])
        assert detectorState.arc[1] == float(exp["det_arc2"])
        assert detectorState.lin[0] == float(exp["det_lin1"])
        assert detectorState.lin[1] == float(exp["det_lin2"])
        assert detectorState.wav == float(exp["BL3:Chop:Skf1:WavelengthUserReq"])
        assert detectorState.freq == float(exp["BL3:Det:TH:BL:Frequency"])
        assert detectorState.guideStat == float(exp["BL3:Mot:OpticsPos:Pos"])
        assert exp == detectorState.getLogValues()
