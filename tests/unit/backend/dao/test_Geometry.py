import unittest

from mantid.simpleapi import CreateWorkspace, SetSample
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry


class TestGeometry(unittest.TestCase):
    def setUp(self):
        self.cylinder = Geometry(
            shape="Cylinder",
            radius=2,
            height=5,
        )
        self.sphere = Geometry(
            shape="Sphere",
            radius=3,
        )

    def test_cylinderGeometry(self):
        # ensure that the geometryDictionary object
        # returns correct dictionary for cylinder
        ref = {
            "Shape": self.cylinder.shape,
            "Radius": self.cylinder.radius,
            "Height": self.cylinder.height,
            "Center": [0, 0, 0],
            "Axis": [0, 1, 0],
        }
        assert self.cylinder.geometryDictionary == ref

    def test_sphereGeometry(self):
        # ensure that the geometryDictionary object
        # returns correct dictionary for sphere
        ref = {
            "Shape": self.sphere.shape,
            "Radius": self.sphere.radius,
            "Center": [0, 0, 0],
        }
        assert self.sphere.geometryDictionary == ref

    def test_settableInMantid(self):
        # test that these can be used to set the sample in mantid
        # run SetSample with the dictionary to set shape
        # then get output XML of the sample shape
        # note that the XML converts from cm to m
        sampleWS = CreateWorkspace(
            DataX=1,
            DataY=1,
        )
        # test setting with a cylinder, compare output XML
        # will have center-of-bottom-base, axis, height, and radius
        SetSample(InputWorkspace=sampleWS, Geometry=self.cylinder.geometryDictionary)
        ref = f"""
        <type name="userShape">
            <{self.cylinder.shape.lower()} id="sample-shape">
                <centre-of-bottom-base x="0" y="{-self.cylinder.height*0.005}" z="0"/>
                <axis x="0" y="1" z="0"/>
                <height val="{self.cylinder.height*0.01}"/>
                <radius val="{self.cylinder.radius*0.01}"/>
            </{self.cylinder.shape.lower()}>
        </type>
        """
        ans = sampleWS.sample().getShape().getShapeXML()
        ref = ref.replace("\n", "").replace(" ", "")
        ans = ans.replace("\n", "").replace(" ", "")
        assert ref == ans
        # test setting with a sphere, compare output XML
        # will have center and radius
        SetSample(
            InputWorkspace=sampleWS,
            Geometry=self.sphere.geometryDictionary,
        )
        ref = f"""
        <type name="userShape">
            <{self.sphere.shape.lower()} id="sphere">
                <center x="0" y="0" z="0"/>
                <radius val="{self.sphere.radius*0.01}"/>
            </{self.sphere.shape.lower()}>
        </type>
        """
        ans = sampleWS.sample().getShape().getShapeXML()
        ref = ref.replace("\n", "").replace(" ", "")
        ans = ans.replace("\n", "").replace(" ", "")
        assert ref == ans