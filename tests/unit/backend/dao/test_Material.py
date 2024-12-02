import json
import unittest

import pytest
from mantid.simpleapi import CreateWorkspace, DeleteWorkspace, SetSample

from snapred.backend.dao.state.CalibrantSample.Material import Material


class TestMaterial(unittest.TestCase):
    def setUp(self):
        self.vanadium = Material(
            chemicalFormula="V",
        )
        self.vanadiumPF = Material(
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
            chemicalFormula="B-C",
        )
        self.vanadiumBoronMD = Material(
            massDensity=0.92,
            chemicalFormula="C-Si",
        )

    def test_isShapedLikeItself(self):
        assert self.vanadium == Material.model_validate(self.vanadium.dict())
        assert self.vanadium == Material.model_validate_json(self.vanadium.json())
        assert self.vanadiumMD == Material.model_validate(self.vanadiumMD.dict())
        assert self.vanadiumMD == Material.model_validate_json(self.vanadiumMD.json())
        assert self.vanadiumPF == Material.model_validate(self.vanadiumPF.dict())
        assert self.vanadiumPF == Material.model_validate_json(self.vanadiumPF.json())
        #
        assert self.vanadiumBoron == Material.model_validate(self.vanadiumBoron.dict())
        assert self.vanadiumBoron == Material.model_validate_json(self.vanadiumBoron.json())
        assert self.vanadiumBoronDash == Material.model_validate(self.vanadiumBoronDash.dict())
        assert self.vanadiumBoronDash == Material.model_validate_json(self.vanadiumBoronDash.json())
        assert self.vanadiumBoronMD == Material.model_validate(self.vanadiumBoronMD.dict())
        assert self.vanadiumBoronMD == Material.model_validate_json(self.vanadiumBoronMD.json())

    def test_singleElementMaterial(self):
        # ensure that the material json object
        # returns correct json for single element

        # single element with formula only
        ref = {
            "chemicalFormula": self.vanadium.chemicalFormula,
        }
        assert self.vanadium.dict() == ref
        assert self.vanadium.json() == json.dumps(ref)

        # single element with packing fraction
        ref = {
            "chemicalFormula": self.vanadiumPF.chemicalFormula,
            "packingFraction": self.vanadiumPF.packingFraction,
        }
        assert self.vanadiumPF.dict() == ref
        assert self.vanadiumPF.json() == json.dumps(ref)
        # single element with mass density
        ref = {
            "chemicalFormula": self.vanadiumMD.chemicalFormula,
            "massDensity": self.vanadiumMD.massDensity,
        }
        assert self.vanadiumMD.dict() == ref
        assert self.vanadiumMD.json() == json.dumps(ref)

    def test_invalidSingleElementMaterial(self):
        with pytest.raises(Warning):
            Material(
                packingFraction=0.3,
                massDensity=0.9,
                chemicalFormula="V",
            )

    def test_twoElementMaterial(self):
        # ensure that the material json object
        # returns correct dictionary for two elements
        ref = {
            "chemicalFormula": self.vanadiumBoron.chemicalFormula,
            "packingFraction": self.vanadiumBoron.packingFraction,
            "massDensity": self.vanadiumBoron.massDensity,
        }
        assert self.vanadiumBoron.dict() == ref
        assert self.vanadiumBoron.json() == json.dumps(ref)
        ref = {
            "chemicalFormula": self.vanadiumBoronDash.chemicalFormula,
            "packingFraction": self.vanadiumBoronDash.packingFraction,
            "massDensity": self.vanadiumBoronDash.massDensity,
        }
        assert self.vanadiumBoronDash.dict() == ref
        assert self.vanadiumBoronDash.json() == json.dumps(ref)

        ref = {
            "chemicalFormula": self.vanadiumBoronMD.chemicalFormula,
            "massDensity": self.vanadiumBoronMD.massDensity,
        }
        assert self.vanadiumBoronMD.dict() == ref
        assert self.vanadiumBoronMD.json() == json.dumps(ref)

    def test_invalidTwoElementMaterial(self):
        # cannot set a multi-element material with only a chemical formula
        with pytest.raises(ValueError):  # noqa: PT011
            Material(
                chemicalFormula="V B",
            )
        # cannot set a multi-element  material with only packing fraction
        with pytest.raises(ValueError):  # noqa: PT011
            Material(
                chemicalFormula="C-Si",
                packingFraction=0.57,
            )

    def test_settableInMantidJSON(self):
        # test that these can be used to set the sample in mantid
        # run SetSample with the json to set shape
        # then get output XML of the sample shape
        # note that the XML converts from cm to m
        sampleWS = CreateWorkspace(
            DataX=1,
            DataY=1,
        )
        # test setting with a single-element crystal
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadium.json(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.packingFraction == 1.0

        #   with packing fraction
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumPF.json(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.packingFraction == self.vanadiumPF.packingFraction

        #   with mass density
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumMD.json(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.packingFraction == 0.11456628477905073

        # test setting with a mutli-element crystal
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumBoron.json(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.chemicalFormula()[0][1].symbol == "B"
        assert material.packingFraction == self.vanadiumBoron.packingFraction
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumBoronDash.json(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "B"
        assert material.chemicalFormula()[0][1].symbol == "C"
        assert material.packingFraction == self.vanadiumBoronDash.packingFraction
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumBoronMD.json(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "C"
        assert material.chemicalFormula()[0][1].symbol == "Si"
        assert material.packingFraction != self.vanadiumBoronDash.packingFraction
        DeleteWorkspace(sampleWS)

    def test_settableInMantidDict(self):
        # test that these can be used to set the sample in mantid
        # run SetSample with the json to set shape
        # then get output XML of the sample shape
        # note that the XML converts from cm to m
        sampleWS = CreateWorkspace(
            DataX=1,
            DataY=1,
        )
        # test setting with a single-element crystal
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadium.dict(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.packingFraction == 1.0

        #   with packing fraction
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumPF.dict(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.packingFraction == self.vanadiumPF.packingFraction

        #   with mass density
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumMD.dict(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.packingFraction == 0.11456628477905073

        # test setting with a mutli-element crystal
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumBoron.dict(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "V"
        assert material.chemicalFormula()[0][1].symbol == "B"
        assert material.packingFraction == self.vanadiumBoron.packingFraction
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumBoronDash.dict(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "B"
        assert material.chemicalFormula()[0][1].symbol == "C"
        assert material.packingFraction == self.vanadiumBoronDash.packingFraction
        SetSample(
            InputWorkspace=sampleWS,
            Material=self.vanadiumBoronMD.dict(),
        )
        material = sampleWS.sample().getMaterial()
        assert material.chemicalFormula()[0][0].symbol == "C"
        assert material.chemicalFormula()[0][1].symbol == "Si"
        assert material.packingFraction != self.vanadiumBoronDash.packingFraction
        DeleteWorkspace(sampleWS)

    def test_invalidIdeasAreInvalid(self):
        """
        This test is for development purposes.
        It shows what material do not work, to inform creation of validation in Material.
        """
        sampleWS = CreateWorkspace(
            DataX=1,
            DataY=1,
        )
        # you cannot set a single-element crystal with packing fraction and mass density
        with pytest.raises(RuntimeError):
            SetSample(
                InputWorkspace=sampleWS,
                Material={
                    "packingFraction": 0.3,
                    "massDensity": 0.9,
                    "chemicalFormula": "V",
                },
            )
        # you cannot set a multi-element crystal with just the formula
        with pytest.raises(RuntimeError):  # noqa: PT011
            SetSample(
                InputWorkspace=sampleWS,
                Material={
                    "chemicalFormula": "C-Si",
                },
            )
        # you cannot set a multi-element crystal with just the formula and packing fraction
        with pytest.raises(RuntimeError):  # noqa: PT011
            SetSample(
                InputWorkspace=sampleWS,
                Material={
                    "chemicalFormula": "C-Si",
                    "packingFraction": 0.11,
                },
            )
        DeleteWorkspace(sampleWS)
