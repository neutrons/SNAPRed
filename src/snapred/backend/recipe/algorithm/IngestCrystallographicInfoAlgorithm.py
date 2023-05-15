from mantid.api import (
    AlgorithmFactory,
    CreateSampleWorkspace,
    LoadCIF,
    PythonAlgorithm,
    mtd,
)
from mantid.geometry import (
    ReflectionConditionFilter,
    ReflectionGenerator,
)
from mantid.kernel import Direction

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "IngestCrystallographicInfo"


class IngestCrystallographicInfo(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("cifPath", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystallographicInfo", defaultValue="", direction=Direction.Output)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        cifPath = self.getProperty("cifPath").value
        # run the algo
        self.log().notice("ingest crystallogtaphic info")

        # Load the CIF file
        CreateSampleWorkspace(OutputWorkspace="xtal_data")
        LoadCIF(Workspace="xtal_data", InputFile=cifPath)
        ws = mtd["xtal_data"]
        xtal = ws.sample().getCrystalStructure()

        # Generate reflections
        generator = ReflectionGenerator(xtal)

        # Create list of unique reflections between 0.7 and 3.0 Angstrom
        # TODO: fix dMin, dMax to specific values
        dMin = 0.5
        dMax = 5.0
        hkls = generator.getUniqueHKLsUsingFilter(dMin, dMax, ReflectionConditionFilter.StructureFactor)

        # Calculate d and F^2
        dValues = generator.getDValues(hkls)
        fSquared = generator.getFsSquared(hkls)
        pg = xtal.getSpaceGroup().getPointGroup()
        multiplicities = [len(pg.getEquivalents(hkl)) for hkl in hkls]

        xtalInfo = CrystallographicInfo(hkl=hkls, d=dValues, fSquared=fSquared, multiplicities=multiplicities)

        self.findFSquaredThreshold(xtalInfo)

    def findFSquaredThreshold(self, xtal: CrystallographicInfo):
        # set a threshold for weak peaks in the spectrum
        # this uses the median of lowest 1% of intensities

        # intensity is fsq times multiplicities
        I0 = [ff * mm for ff, mm in zip(xtal.fSquared, xtal.multiplicities)]
        I0.sort()

        # take lowest one percent
        numPeaks = len(xtal.fSquared)
        lowest = max(1, round(numPeaks / 100)) - 1
        return I0[int(lowest / 2)]


# Register algorithm with Mantid
AlgorithmFactory.subscribe(IngestCrystallographicInfo)
