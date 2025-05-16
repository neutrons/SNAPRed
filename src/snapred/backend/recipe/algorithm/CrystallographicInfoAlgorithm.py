from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.geometry import (
    CrystalStructure,
    ReflectionConditionFilter,
    ReflectionGenerator,
)
from mantid.kernel import Direction

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty


class CrystallographicInfoAlgorithm(PythonAlgorithm):
    @classproperty
    def D_MIN(cls):
        return Config["constants.CrystallographicInfo.crystalDMin"]

    @classproperty
    def D_MAX(cls):
        return Config["constants.CrystallographicInfo.crystalDMax"]

    def category(self):
        return "SNAPRed Sample Data"

    def PyInit(self):
        # declare properties
        self.declareProperty("CifPath", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystalInfo", defaultValue="", direction=Direction.Output)
        self.declareProperty("Crystallography", defaultValue="", direction=Direction.InOut)
        self.declareProperty("crystalDMin", defaultValue=self.D_MIN, direction=Direction.Input)
        self.declareProperty("crystalDMax", defaultValue=self.D_MAX, direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def PyExec(self):
        cifPath = self.getProperty("CifPath").value
        # run the algo
        self.log().notice("ingest crystallogtaphic info")

        # Load the CIF file into an empty workspace
        # use this to set the crystallography output
        # also create a mantid CrystalStructure object
        if not self.getProperty("CifPath").isDefault:
            ws = "xtal_data"
            self.mantidSnapper.CreateSingleValuedWorkspace(
                "Creating sample workspace...",
                OutputWorkspace=ws,
            )
            self.mantidSnapper.LoadCIF("Loading crystal data...", Workspace=ws, InputFile=cifPath)
            self.mantidSnapper.executeQueue()
            ws = self.mantidSnapper.mtd[ws]
            xtal = ws.sample().getCrystalStructure()
            xtallography = Crystallography(cifPath, xtal)
        elif not self.getProperty("Crystallography").isDefault:
            xtallography = Crystallography.model_validate_json(self.getPropertyValue("Crystallography"))
            xtal = CrystalStructure(
                xtallography.unitCellString,
                xtallography.spaceGroupString,
                xtallography.scattererString,
            )
        else:
            raise ValueError("Either cifPath or crystalStructure must be set!")
        self.setPropertyValue("Crystallography", xtallography.json())

        # Generate reflections
        generator = ReflectionGenerator(xtal)

        # Create list of unique reflections between 0.1 and 100.0 Angstrom
        crystalDMin = self.getProperty("crystalDMin").value
        crystalDMax = self.getProperty("crystalDMax").value
        hkls = generator.getUniqueHKLsUsingFilter(crystalDMin, crystalDMax, ReflectionConditionFilter.StructureFactor)

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

        xtalInfo = CrystallographicInfo(hkl=hkls, dSpacing=dValues, fSquared=fSquared, multiplicities=multiplicities)
        self.setPropertyValue("CrystalInfo", xtalInfo.json())
        # ws
        self.mantidSnapper.WashDishes(
            "Cleaning up xtal workspace.",
            Workspace="xtal_data",
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CrystallographicInfoAlgorithm)
