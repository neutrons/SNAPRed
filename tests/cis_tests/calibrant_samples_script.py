# note: this runs the same checks as the unit test of CalibrantSample

from mantid.simpleapi import CreateWorkspace, SetSample
from mantid.geometry import CrystalStructure
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material
from snapred.backend.dao.state.CalibrantSample.Atom import Atom

mat = Material(chemicalFormula="(Li7)2-C-H4-N-Cl6", massDensity=4.4, packingFraction=0.9)
geo = Geometry(shape="Cylinder", radius=0.1, height=3.6, center=[0.0, 0.0, 0.0])
atom = Atom(symbol="Si", coordinates=[0.125, 0.125, 0.125], siteOccupationFactor=1.0)
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

# make sure it worked
ref = f"""
<type name="userShape">
    <{geo.shape.lower()} id="sample-shape">
        <centre-of-bottom-base x="0" y="{-round(geo.height*0.005,3)}" z="0"/>
        <axis x="0" y="1" z="0"/>
        <height val="{round(geo.height*0.01,3)}"/>
        <radius val="{round(geo.radius*0.01,3)}"/>
    </{geo.shape.lower()}>
</type>
"""
ans = ws.sample().getShape().getShapeXML()
ref = ref.replace("\n", "").replace(" ", "")
ans = ans.replace("\n", "").replace(" ", "")
assert ref == ans

# Creating a CrystalStructure from info from Crystallography object should not fail
crystalStruct = CrystalStructure(crystal.unitCellString, crystal.spaceGroupString, crystal.scattererString)

# make sure it worked
assert crystal.spaceGroup == crystalStruct.getSpaceGroup().getHMSymbol()
for i, atom in enumerate(crystal.atoms):
    atomstring = atom.getString.split(' ')
    xtalstring = crystalStruct.getScatterers()[i].split(' ')
    assert len(atomstring) == len(xtalstring)
    assert atomstring[0] == xtalstring[0]
    for a,x in zip(atomstring[1:],xtalstring[1:]):
        assert float(a)==float(x)
