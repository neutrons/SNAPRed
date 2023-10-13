# note: this runs the same checks as the calibrant_samples_script CIS test

import unittest

import pytest
from mantid.geometry import CrystalStructure
from mantid.simpleapi import CreateWorkspace, DeleteWorkspace, SetSample
from snapred.backend.dao.state.CalibrantSample.Atom import Atom
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material
from snapred.meta.Config import Resource


class TestCalibrantSamples(unittest.TestCase):
    def setUp(self):
        self.geo = Geometry(shape="Cylinder", radius=0.1, height=3.6, center=[0.0, 0.0, 0.0])
        self.mat = Material(chemicalFormula="(Li7)2-C-H4-N-Cl6", massDensity=4.4, packingFraction=0.9)
        self.atom = Atom(symbol="Si", coordinates=[0.125, 0.125, 0.125], siteOccupationFactor=1.0)
        self.xtal = Crystallography(
            cifFile=Resource.getPath("inputs/crystalInfo/example.cif"),
            spaceGroup="F d -3 m",
            latticeParameters=[5.43159, 5.43159, 5.43159, 90.0, 90.0, 90.0],
            atoms=[self.atom, self.atom, self.atom],
        )
        self.sample = CalibrantSamples(
            name="NIST_640D",
            unique_id="001",
            geometry=self.geo,
            material=self.mat,
            crystallography=self.xtal,
        )
        ws = CreateWorkspace(DataX=1, DataY=1)
        self.ws = ws

    def tearDown(self) -> None:
        DeleteWorkspace(self.ws)
        return super().tearDown()

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
                <centre-of-bottom-base x="0" y="{-round(self.geo.height*0.005,3)}" z="0"/>
                <axis x="0" y="1" z="0"/>
                <height val="{round(self.geo.height*0.01,3)}"/>
                <radius val="{round(self.geo.radius*0.01,3)}"/>
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

    def testSetCrystalStructure(self):
        crystalStruct = CrystalStructure(
            self.xtal.unitCellString,
            self.xtal.spaceGroupString,
            self.xtal.scattererString,
        )

        # make sure it worked
        assert self.xtal.spaceGroup == crystalStruct.getSpaceGroup().getHMSymbol()
        for i, atom in enumerate(self.xtal.atoms):
            atomstring = atom.getString.split(" ")
            xtalstring = crystalStruct.getScatterers()[i].split(" ")
            assert len(atomstring) == len(xtalstring)
            assert atomstring[0] == xtalstring[0]
            for a, x in zip(atomstring[1:], xtalstring[1:]):
                assert float(a) == float(x)
