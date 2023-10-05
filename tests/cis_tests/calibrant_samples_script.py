from mantid.simpleapi import *
from mantid.geometry import CrystalStructure
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material
from snapred.backend.dao.state.CalibrantSample.Atom import Atom

mat = Material(chemicalFormula="(Li7)2-C-H4-N-Cl6", massDensity=4.4, packingFraction=0.9)
geo = Geometry(shape="Cylinder", radius=0.1, height=3.6, center=[0.0, 0.0, 0.0])
atom = Atom(atom_type="Si", atom_coordinates=[0.125, 0.125, 0.125], site_occupation_factor=1.0)
crystal = Crystallography(
    cifFile="/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif",
    spaceGroup="F d -3 m",
    latticeParameters=[5.43159, 5.43159, 5.43159, 90.0, 90.0, 90.0],
    atoms=[atom, atom, atom],
)
sample = CalibrantSamples(name="NIST_640D", unique_id="001", geometry=geo, material=mat, crystallography=crystal)
# Calling SetSample() with Geometry and Material from CalibrantSamples object should not fail
ws = CreateWorkspace(DataX=1, DataY=1)
SetSample(ws, Geometry=sample.geometry.json(), Material=sample.material.json())

# Creating a CrystalStructure from info from Crystallography object should not fail
crystalStruct = CrystalStructure(crystal.unitCellString, crystal.spaceGroupString, crystal.scattererString)
