from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.geometry import (
    ReflectionConditionFilter,
    ReflectionGenerator,
)
from mantid.kernel import Direction

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class IngestCrystallographicInfoAlgorithm(PythonAlgorithm):
    # __name__ = "IngestCrystallographicInfoAlgorithm"

    def PyInit(self):
        # declare properties
        self.declareProperty("cifPath", defaultValue="", direction=Direction.Input)
        self.declareProperty("crystalInfo", defaultValue="", direction=Direction.Output)
        self.declareProperty("dMin", defaultValue=0.1, direction=Direction.Input)
        self.declareProperty("dMax", defaultValue=100.0, direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def PyExec(self):
        cifPath = self.getProperty("cifPath").value
        # run the algo
        self.log().notice("ingest crystallogtaphic info")

        # Load the CIF file into an empty workspace

        ws = "xtal_data"
        self.mantidSnapper.CreateWorkspace(
            "Creating sample workspace...",
            OutputWorkspace=ws,
            DataX=1,
            DataY=1,
        )
        self.mantidSnapper.LoadCIF("Loading crystal data...", Workspace=ws, InputFile=cifPath)
        self.mantidSnapper.executeQueue()

        ws = self.mantidSnapper.mtd[ws]
        xtal = ws.sample().getCrystalStructure()

        # Generate reflections
        generator = ReflectionGenerator(xtal)

        # Create list of unique reflections between 0.1 and 100.0 Angstrom
        dMin = self.getProperty("dMin").value
        dMax = self.getProperty("dMax").value
        hkls = generator.getUniqueHKLsUsingFilter(dMin, dMax, ReflectionConditionFilter.StructureFactor)

        # Calculate d and F^2
        dValues = generator.getDValues(hkls)
        fSquared = generator.getFsSquared(hkls)
        pg = xtal.getSpaceGroup().getPointGroup()
        multiplicities = [len(pg.getEquivalents(hkl)) for hkl in hkls]

        # ensure correct types for validation
        # TODO: figure out why this is necessary
        hkls = [tuple(list(hkl)) for hkl in hkls]
        dValues = [float(d) for d in dValues]
        fSquared = [float(fsq) for fsq in fSquared]

        xtal = CrystallographicInfo(hkl=hkls, dSpacing=dValues, fSquared=fSquared, multiplicities=multiplicities)
        self.setProperty("crystalInfo", xtal.json())
        # ws
        self.mantidSnapper.DeleteWorkspace("Cleaning up xtal workspace.", Workspace="xtal_data")
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(IngestCrystallographicInfoAlgorithm)
