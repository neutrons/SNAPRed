import unittest

import pytest
from mantid.simpleapi import CreateWorkspace, SetSample
from snapred.backend.dao.state.CalibrantSample.Material import Material


class TestMaterial(unittest.TestCase):
    def setUp(self):
        self.vanadium = Material(
            packingFraction=0.3,
            chemicalFormula="V",
        )
        self.vanadiumMD = Material(
            massDensity=0.7,
            chemicalFormula="V",
        )
        self.vanadiumBoron = Material(
            packingFraction=0.3,
            massDensity=0.9,
            chemicalFormula="V B",
        )
        self.vanadiumBoronDash = Material(
            packingFraction=0.3,
            massDensity=0.9,
            chemicalFormula="V-B",
        )

    def test_singleElementMaterial(self):
        # ensure that the materialDictionary object
        # returns correct dictionary for single element

        # single element with packing fraction
        ref = {
            "ChemicalFormula": self.vanadium.chemicalFormula,
            "PackingFraction": self.vanadium.packingFraction,
        }
        assert self.vanadium.materialDictionary == ref
        # try single element with mass density
        ref = {
            "ChemicalFormula": self.vanadiumMD.chemicalFormula,
            "MassDensity": self.vanadiumMD.massDensity,
        }
        assert self.vanadiumMD.materialDictionary == ref

    def test_invalidSingleElementMaterial(self):
        with pytest.raises(Warning):
            Material(
                packingFraction=0.3,
                massDensity=0.9,
                chemicalFormula="V",
            )

    def test_invalidTwoElementMaterial(self):
        with pytest.raises(ValueError):  # noqa: PT011
            Material(
                packingFraction=0.3,
                chemicalFormula="V B",
            )

    def test_twoElementMaterial(self):
        # ensure that the materialDictionary object
        # returns correct dictionary for two elements
        ref = {
            "ChemicalFormula": self.vanadiumBoron.chemicalFormula,
            "MassDensity": self.vanadiumBoron.massDensity,
            "PackingFraction": self.vanadiumBoron.packingFraction,
        }
        assert self.vanadiumBoron.materialDictionary == ref

    def test_settableInMantid(self):
        # test that these can be used to set the sample in mantid
        # run SetSample with the dictionary to set shape
        # then get output XML of the sample shape
        # note that the XML converts from cm to m
        sampleWS = CreateWorkspace(
            DataX=1,
            DataY=1,
        )
        # test setting with a single-element crystal
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadium.materialDictionary,
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.packingFraction == self.vanadium.packingFraction

        # test setting with a single-element crystal with mass density
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumMD.materialDictionary,
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"

        # test setting with a mutli-element crystal
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumBoron.materialDictionary,
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.chemicalFormula()[0][1].symbol == "B"
        assert material.packingFraction == self.vanadiumBoron.packingFraction

        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumBoronDash.materialDictionary,
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.chemicalFormula()[0][1].symbol == "B"
        assert material.packingFraction == self.vanadiumBoronDash.packingFraction
