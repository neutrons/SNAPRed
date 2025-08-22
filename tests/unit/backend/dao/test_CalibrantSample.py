# note: this runs the same checks as the calibrant_samples_script CIS test
import unittest

from mantid.simpleapi import CreateWorkspace, DeleteWorkspace, SetSample
from util.dao import DAOFactory

from snapred.backend.dao.state.CalibrantSample.CalibrantSample import CalibrantSample
from snapred.meta.Time import timestamp


class TestCalibrantSample(unittest.TestCase):
    def setUp(self):
        self.geo = DAOFactory.sample_geometry_cylinder
        self.mat = DAOFactory.sample_material

        self.sample = DAOFactory.sample_calibrant_sample
        ws = CreateWorkspace(DataX=1, DataY=1)
        self.ws = ws

    def tearDown(self) -> None:
        DeleteWorkspace(self.ws)
        return super().tearDown()

    def test_isShapedLikeItself(self):
        self.sample.date = timestamp()
        sampleFromDict = CalibrantSample(**self.sample.dict())
        sampleFromJSON = CalibrantSample.model_validate_json(self.sample.model_dump_json())
        assert self.sample == sampleFromDict
        assert self.sample == sampleFromJSON

    def test_setCalibrantSample(self):
        SetSample(
            self.ws,
            Geometry=self.sample.geometry.json(),
            Material=self.sample.material.json(),
        )

        # make sure it worked
        ref = f"""
        <type name="userShape">
            <{self.geo.shape.lower()} id="sample-shape">
                <centre-of-bottom-base x="0" y="{-round(self.geo.height * 0.005, 3)}" z="0"/>
                <axis x="0" y="1" z="0"/>
                <height val="{round(self.geo.height * 0.01, 3)}"/>
                <radius val="{round(self.geo.radius * 0.01, 3)}"/>
            </{self.geo.shape.lower()}>
        </type>
        """
        ans = self.ws.sample().getShape().getShapeXML()
        ref = ref.replace("\n", "").replace(" ", "")
        ans = ans.replace("\n", "").replace(" ", "")
        assert ref == ans

        material = self.ws.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "Li"
        assert material.chemicalFormula()[0][1].symbol == "C"
        assert material.chemicalFormula()[0][2].symbol == "H"
        assert material.chemicalFormula()[0][3].symbol == "N"
        assert material.chemicalFormula()[0][4].symbol == "Cl"
        assert material.packingFraction == self.mat.packingFraction

    def test_chop_calibrant_sample(self):
        # removed from test of Raw Vanadium Correction

        # chop and verify the spherical sample
        sampleGeometry = DAOFactory.fake_sphere_sample.geometry.dict()
        sampleMaterial = DAOFactory.fake_sphere_sample.material.dict()
        assert sampleGeometry["shape"] == "Sphere"
        assert sampleGeometry["radius"] == DAOFactory.fake_sphere.radius
        assert sampleGeometry["center"] == [0, 0, 0]
        assert sampleMaterial["chemicalFormula"] == DAOFactory.fake_material.chemicalFormula

        # chop and verify the cylindrical sample
        sampleGeometry = DAOFactory.fake_cylinder_sample.geometry.dict()
        sampleMaterial = DAOFactory.fake_cylinder_sample.material.dict()
        assert sampleGeometry["shape"] == "Cylinder"
        assert sampleGeometry["radius"] == DAOFactory.fake_cylinder.radius
        assert sampleGeometry["height"] == DAOFactory.fake_cylinder.height
        assert sampleGeometry["center"] == [0, 0, 0]
        assert sampleGeometry["axis"] == [0, 1, 0]
        assert sampleMaterial["chemicalFormula"] == DAOFactory.fake_material.chemicalFormula
