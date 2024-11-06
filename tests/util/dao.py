import json
from random import randint

from snapred.backend.dao import CrystallographicInfo, CrystallographicPeak
from snapred.backend.dao.calibration import Calibration, CalibrationMetric, CalibrationRecord, FocusGroupMetric
from snapred.backend.dao.GSASParameters import GSASParameters
from snapred.backend.dao.ingredients import PeakIngredients
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import BinnedValue, Limit
from snapred.backend.dao.normalization import Normalization, NormalizationRecord
from snapred.backend.dao.ObjectSHA import ObjectSHA
from snapred.backend.dao.ParticleBounds import ParticleBounds
from snapred.backend.dao.state.CalibrantSample.Atom import Atom
from snapred.backend.dao.state.CalibrantSample.CalibrantSample import CalibrantSample
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.GroupingMap import GroupingMap
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.meta.Config import Resource
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf


class DAOFactory:

    ## DETECTOR STATE

    real_detector_state = DetectorState(
        arc=(-65.3, 104.95),
        wav=2.1,
        freq=60.0,
        guideStat=1,
        lin=(0.045, 0.043),
    )

    unreal_detector_state = DetectorState(  # TODO remove this object?
        arc=(1, 2),
        wav=1.1,
        freq=1.2,
        guideStat=1,
        lin=(1, 2),
    )

    ## STATE ID
    
    # state-id corresponding to `real_detector_state`:
    real_state_id = ObjectSHA(
        hex="04bd2c53f6bf6754",
        decodedKey="{'vdet_arc1': -65.5, 'vdet_arc2': 105.0, 'WavelengthUserReq': 2.1, 'Frequency': 60, 'Pos': 1}",
    )
    
    magical_state_id = ObjectSHA(
        hex="0a1b2c3d0a1b2c3d",
        decodedKey=None,
    )

    pv_state_id = ObjectSHA(
        hex="ab8704b0bc2a2342",
        decodedKey=None,
    )

    nonsense_state_id = ObjectSHA(
        hex="aabbbcccdddeeeff",
        decodedKey=None,
    )

    ## INSTRUMENT CONFIG

    instrument_config_boilerplate = {
        "version": "1.4",
        "facility": "SNS",
        "name": "SNAP",
        "nexusFileExtension": ".nxs.h5",
        "nexusFilePrefix": "SNAP_",
        "calibrationFileExtension": "json",
        "calibrationFilePrefix": "SNAPcalibLog",
        "calibrationDirectory": "/SNS/SNAP/shared/Calibration/",
        "pixelGroupingDirectory": "PixelGroupingDefinitions/",
        "sharedDirectory": "shared/",
        "nexusDirectory": "nexus/",
        "reducedDataDirectory": "shared/manualReduced/",
        "reductionRecordDirectory": "shared/manualReduced/reductionRecord/",
        "bandwidth": "3.0",
        "L1": 15.0,
        "L2": 0.5,
        "delTOverT": 0.002,
        "delThWithGuide": 0.0032,
        "width": 1600.0,
        "frequency": 60.4,
    }

    default_instrument_config = InstrumentConfig(
        **instrument_config_boilerplate,
        maxBandwidth=3.2,
        delLOverL=6.452e-05,
        delThNoGuide=0.0006667,
    )

    other_instrument_config = InstrumentConfig(
        **instrument_config_boilerplate,
        maxBandwidth=3.0,
        delLOverL=6.453e-05,
        delThNoGuide=0.0032,
    )

    ## GSAS PARAMETERS

    real_gsas_params = GSASParameters(
        alpha=0.1,
        beta=(0.02, 0.05),
    )

    unreal_gsas_params = GSASParameters(
        alpha=1.1,
        beta=(1, 2),
    )

    ## PARTICLE BOUNDS

    # TODO are ALL of these absolutely necessary?

    particle_bounds_1 = ParticleBounds.model_validate(
        {
            "wavelength": {"minimum": -0.3499999999999999, "maximum": 2.6},
            "tof": {"minimum": -1371.3229026935753, "maximum": 10186.970134295134},
        }
    )
    particle_bounds_2 = ParticleBounds.model_validate(
        {
            "wavelength": {"minimum": 0.7, "maximum": 3.5},
            "tof": {"minimum": 2742.6, "maximum": 13713},
        }
    )
    particle_bounds_3 = ParticleBounds.model_validate(
        {
            "wavelength": {"minimum": -0.3999999999999999, "maximum": 2.6},
            "tof": {"minimum": 1567.2000202219353, "maximum": 10186.800131442584},
        }
    )
    particle_bounds_7 = ParticleBounds.model_validate(
        {
            "wavelength": {"minimum": 0.60, "maximum": 3.60},
            "tof": {"minimum": 1959.0, "maximum": 14496.6},
        }
    )
    particle_bounds_pop = ParticleBounds(
        # fake instrument state . json
        wavelength=Limit(minimum=0, maximum=2.6),
        tof=Limit(minimum=10, maximum=1000),
    )

    ## INSTRUMENT STATE

    default_instrument_state = InstrumentState(
        id=magical_state_id,
        instrumentConfig=other_instrument_config,
        detectorState=real_detector_state,
        gsasParameters=real_gsas_params,
        particleBounds=particle_bounds_1,
        fwhmMultipliers=Limit(minimum=2, maximum=2),
        defaultGroupingSliceValue=5.0,
        peakTailCoefficient=2.0,
    )

    # POP
    pop_instrument_state = InstrumentState(
        id=magical_state_id,
        instrumentConfig=other_instrument_config,
        detectorState=real_detector_state,
        gsasParameters=real_gsas_params,
        particleBounds=particle_bounds_7,
        fwhmMultipliers=Limit(minimum=1.5, maximum=1.5),
        defaultGroupingSliceValue=5.0,
        peakTailCoefficient=2.0,
    )

    # PGP

    pgp_instrument_state = InstrumentState(
        id=magical_state_id,
        instrumentConfig=default_instrument_config,  # TODO can be changed to other_?
        detectorState=real_detector_state,
        gsasParameters=real_gsas_params,
        particleBounds=particle_bounds_2,
        fwhmMultipliers=Limit(minimum=1.3, maximum=1.3),
        defaultGroupingSliceValue=5.0,
        peakTailCoefficient=2.0,
    )

    sample_instrument_state = InstrumentState(
        id=magical_state_id,
        instrumentConfig=other_instrument_config,
        detectorState=unreal_detector_state,
        gsasParameters=unreal_gsas_params,
        particleBounds=particle_bounds_3,
        fwhmMultipliers=Limit(minimum=2, maximum=2),
        defaultGroupingSliceValue=5.0,
        peakTailCoefficient=2.0,
    )

    # SyntheticData
    synthetic_instrument_state = InstrumentState(
        # fake instrument state . json
        id=magical_state_id,
        instrumentConfig=other_instrument_config,
        detectorState=unreal_detector_state,
        gsasParameters=unreal_gsas_params,
        particleBounds=particle_bounds_pop,
        fwhmMultipliers=Limit(minimum=1, maximum=2),
        defaultGroupingSliceValue=5.0,
        peakTailCoefficient=1.0,
    )

    # For PV calculations
    pv_instrument_state = InstrumentState(
        id=pv_state_id,
        instrumentConfig=other_instrument_config,
        detectorState=unreal_detector_state,
        gsasParameters=unreal_gsas_params,
        particleBounds=particle_bounds_1,
        fwhmMultipliers=Limit(minimum=2, maximum=2),
        defaultGroupingSliceValue=5.0,
        peakTailCoefficient=2.0,
    )

    ## FOCUS GROUPS

    synthetic_focus_group_natural = FocusGroup(
        name="Natural",
        definition="fakeSNAPFocGroup_Natural.xml",
    )

    focus_group_POP_natural_native = FocusGroup(
        name="Natural",
        definition="fakeSNAPFocGroup_Natural.xml",
    )

    focus_group_POP_column_native = FocusGroup(
        name="Column",
        definition="fakeSNAPFocGroup_Column.xml",
    )

    focus_group_SNAP_all_native = FocusGroup(
        name="All",
        definition="SNAPFocGroup_All.hdf",
    )

    focus_group_SNAP_bank_native = FocusGroup(
        name="Bank",
        definition="SNAPFocGroup_Bank.hdf",
    )

    focus_group_SNAP_column_native = FocusGroup(
        name="Column",
        definition="SNAPFocGroup_Column.hdf",
    )

    focus_group_SNAP_all_lite = FocusGroup(
        name="All",
        definition="SNAPFocGroup_All.lite.hdf",
    )

    focus_group_SNAP_bank_lite = FocusGroup(
        name="Bank",
        definition="SNAPFocGroup_Bank.lite.hdf",
    )

    focus_group_SNAP_column_lite = FocusGroup(
        name="Column",
        definition="NAPFocGroup_Column.lite.hdf",
    )

    ## GROUPING MAP

    @classmethod
    def groupingMap_SNAP(cls, stateId=magical_state_id.copy()) -> GroupingMap:
        return GroupingMap(
            stateId=stateId,
            liteFocusGroups=[
                cls.focus_group_SNAP_all_lite.copy(),
                cls.focus_group_SNAP_bank_lite.copy(),
                cls.focus_group_SNAP_column_lite.copy(),
            ],
            nativeFocusGroups=[
                cls.focus_group_SNAP_all_native.copy(),
                cls.focus_group_SNAP_bank_native.copy(),
                cls.focus_group_SNAP_column_native.copy(),
            ],
        )

    @classmethod
    def groupingMap_POP(cls, stateId=magical_state_id.copy()) -> GroupingMap:
        return GroupingMap(
            stateId=stateId,
            liteFocusGroups=[],
            nativeFocusGroups=[
                cls.focus_group_POP_column_native.copy(),
                cls.focus_group_POP_natural_native.copy(),
            ],
        )

    ## CRYSTALLOGRAPHIC INFO

    @classmethod
    def xtalInfo(cls, runNumber: str, useLiteMode: bool):  # noqa ARG003
        return cls.default_xtal_info.copy()

    default_xtal_peak = CrystallographicPeak(
        hkl=(0, 0, 0),
        dSpacing=0.0,
        fSquared=0.0,
        multiplicity=4,
    )

    default_xtal_info = CrystallographicInfo(
        peaks=[default_xtal_peak] * 6,
    )

    good_xtal_info = CrystallographicInfo(peaks=json.loads(Resource.read("inputs/predict_peaks/good_peak_list.json")))

    default_xtal_dbounds = Limit[float](
        minimum=1.0,
        maximum=2.0,
    )

    ## PIXEL GROUPS

    synthetic_pixel_group = PixelGroup(
        # fake pixel group . json
        focusGroup=synthetic_focus_group_natural.copy(),
        timeOfFlight=BinnedValue(
            minimum=10,
            maximum=1000,
            binWidth=0.0002771822370612031,
            binningMode=-1,
        ),
        pixelGroupingParameters=[
            PixelGroupingParameters(
                groupID=3,
                isMasked=False,
                L2=10.0,
                twoTheta=2.1108948177427838,
                azimuth=0.0,
                dResolution=Limit(
                    minimum=0.06,
                    maximum=0.41,
                ),
                dRelativeResolution=0.002771822370612031,
            ),
            PixelGroupingParameters(
                groupID=7,
                isMasked=False,
                L2=10.0,
                twoTheta=1.82310673131693,
                azimuth=0.0,
                dResolution=Limit(
                    minimum=0.10,
                    maximum=0.64,
                ),
                dRelativeResolution=0.003234525153614593,
            ),
            PixelGroupingParameters(
                groupID=2,
                isMasked=False,
                L2=10.0,
                twoTheta=1.5352228379083572,
                azimuth=0.0,
                dResolution=Limit(
                    minimum=0.05,
                    maximum=0.36,
                ),
                dRelativeResolution=0.004014748859873182,
            ),
            PixelGroupingParameters(
                groupID=11,
                isMasked=False,
                L2=10.0,
                twoTheta=1.440276389170165,
                azimuth=0.0,
                dResolution=Limit(
                    minimum=0.07,
                    maximum=0.48,
                ),
                dRelativeResolution=0.004309856397320412,
            ),
        ],
    )

    good_pixel_group = PixelGroup(
        focusGroup=synthetic_focus_group_natural.copy(),
        timeOfFlight=BinnedValue(
            minimum=1959.0,
            maximum=14496.6,
            binWidth=0.01,
            binningMode=-1,
        ),
        pixelGroupingParameters=[
            PixelGroupingParameters(
                groupID=11,
                isMasked=False,
                L2=10.0,
                twoTheta=2.1108948177427838,
                azimuth=0.0,
                dResolution=Limit(
                    minimum=0.2873164190984029,
                    maximum=2.1261415013281812,
                ),
                dRelativeResolution=0.002699865774827431,
            ),
            {
                "groupID": 2,
                "isMasked": False,
                "L2": 10.0,
                "twoTheta": 1.82310673131693,
                "azimuth": 0.0,
                "dResolution": {"minimum": 0.31627307109869773, "maximum": 2.340420726130363},
                "dRelativeResolution": 0.0031863552001173434,
            },
            {
                "groupID": 7,
                "isMasked": False,
                "L2": 10.0,
                "twoTheta": 1.5352228379083572,
                "azimuth": 0.0,
                "dResolution": {"minimum": 0.36001346277099444, "maximum": 2.6640996245053588},
                "dRelativeResolution": 0.003872908058770528,
            },
            {
                "groupID": 8,
                "isMasked": False,
                "L2": 10.0,
                "twoTheta": 1.440276389170165,
                "azimuth": 0.0,
                "dResolution": {"minimum": 0.3790816270355628, "maximum": 2.8052040400631646},
                "dRelativeResolution": 0.004160341882643039,
            },
            {
                "groupID": 22,
                "isMasked": False,
                "L2": 10.0,
                "twoTheta": 1.1543988461042238,
                "azimuth": 0.0,
                "dResolution": {"minimum": 0.4581446222528327, "maximum": 3.3902702046709625},
                "dRelativeResolution": 0.0053059972420510075,
            },
            {
                "groupID": 106,
                "isMasked": False,
                "L2": 10.0,
                "twoTheta": 0.8690010092470938,
                "azimuth": 0.0,
                "dResolution": {"minimum": 0.5938843544407365, "maximum": 4.39474422286145},
                "dRelativeResolution": 0.007179854355740084,
            },
        ],
    )

    @classmethod
    def pixelGroups(cls):
        return [cls.synthetic_pixel_group.copy()]

    ## PEAK INGREDIENTS

    fake_peak_ingredients = PeakIngredients(
        instrumentState=pop_instrument_state.copy(),
        peakIntensityThreshold=1,
        pixelGroup=synthetic_pixel_group.copy(),
        crystalInfo=default_xtal_info.copy(),
    )

    good_peak_ingredients = PeakIngredients(
        instrumentState=pop_instrument_state.copy(),
        peakIntensityThreshold=0.05,
        pixelGroup=good_pixel_group.copy(),
        crystalInfo=good_xtal_info.copy(),
    )

    ## CALIBRATION PARAMETERS

    @classmethod
    def calibrationParameters(
        cls,
        runNumber: str = str(randint(5000, 10000)),
        useLiteMode: bool = True,
        version: int = randint(2, 120),
        *,
        creationDate="2024-04-03",
        name="test",
        instrumentState=default_instrument_state.copy(),
    ) -> Calibration:
        return Calibration(
            seedRun=runNumber,
            useLiteMode=useLiteMode,
            version=version,
            creationDate=creationDate,
            name=name,
            instrumentState=instrumentState,
        )

    ## CALIBRATION RECORD

    @classmethod
    def calibrationRecord(
        cls,
        runNumber: str = str(randint(5000, 10000)),
        useLiteMode: bool = True,
        version: int = randint(2, 120),
        **other_properties,
    ) -> CalibrationRecord:
        other_properties.setdefault("crystalInfo", cls.default_xtal_info.copy())
        other_properties.setdefault("pixelGroups", cls.pixelGroups())
        other_properties.setdefault(
            "workspaces",
            {
                "diffCalOutput": [f"_dsp_column_{wnvf.formatRunNumber(runNumber)}_{wnvf.formatVersion(version)}"],
                "diffCalDiagnostic": [
                    f"_diagnostic_column_{wnvf.formatRunNumber(runNumber)}_{wnvf.formatVersion(version)}"
                ],
                "diffCalTable": [f"_diffract_consts_{wnvf.formatRunNumber(runNumber)}_{wnvf.formatVersion(version)}"],
                "diffCalMask": [
                    f"_diffract_consts_mask_{wnvf.formatRunNumber(runNumber)}_{wnvf.formatVersion(version)}"
                ],
            },
        )
        other_properties.setdefault("calculationParameters", cls.calibrationParameters(runNumber, useLiteMode, version))
        other_properties.setdefault("focusGroupCalibrationMetrics", cls.focusGroupCalibrationMetric_Column.copy())
        return CalibrationRecord(
            runNumber=runNumber,
            useLiteMode=useLiteMode,
            version=version,
            **other_properties,
        )

    ## NORMALIZATION PARAMETERS

    @classmethod
    def normalizationParameters(
        cls,
        runNumber: str = "",
        useLiteMode: bool = True,
        version: int = -2,
        *,
        creationDate="2024-04-03",
        name="test",
        instrumentState=default_instrument_state.copy(),
    ) -> Normalization:
        if runNumber == "":
            runNumber = str(randint(5000, 10000))
        if version == -2:
            version = randint(2, 120)
        return Normalization(
            seedRun=runNumber,
            useLiteMode=useLiteMode,
            version=version,
            creationDate=creationDate,
            name=name,
            instrumentState=instrumentState,
        )

    ## NORMALIZATION RECORD

    @classmethod
    def normalizationRecord(
        cls,
        runNumber: str = "",
        useLiteMode: bool = True,
        version: int = -2,
        **other_properties,
    ) -> NormalizationRecord:
        if runNumber == "":
            runNumber = str(randint(5000, 10000))
        if version == -2:
            version = randint(2, 120)
        other_properties.setdefault("backgroundRunNumber", runNumber)
        other_properties.setdefault("smoothingParameter", randint(1, 100) / 100.0)
        other_properties.setdefault(
            "workspaceNames",
            [
                "fitted_strippedFocussedData_stripped_0",
                "fitted_strippedFocussedData_stripped_1",
                "fitted_strippedFocussedData_stripped_2",
            ],
        )
        other_properties.setdefault("calibrationVersionUsed", randint(2, 120))
        other_properties.setdefault("crystalDBounds", cls.default_xtal_dbounds.copy())
        other_properties.setdefault(
            "calculationParameters", cls.normalizationParameters(runNumber, useLiteMode, version)
        )
        other_properties.setdefault("normalizationCalibrantSamplePath", "fakePath")

        return NormalizationRecord(
            runNumber=runNumber,
            useLiteMode=useLiteMode,
            version=version,
            **other_properties,
        )

    ## FOCUS GROUP CALIBRATION METRICS

    default_calibration_metric = CalibrationMetric(
        sigmaAverage=0.002517952998136214,
        sigmaStandardDeviation=0.00000878421857231147,
        strainAverage=0.1515216185900471,
        strainStandardDeviation=0.013151225165758604,
        twoThetaAverage=2.110894794556694,
    )

    focusGroupCalibrationMetric_Column = FocusGroupMetric(
        focusGroupName="Column",
        calibrationMetric=[default_calibration_metric.copy()] * 6,
    )

    ## CalibrantSample

    # Geometry
    sample_geometry_cylinder = Geometry(shape="Cylinder", radius=0.1, height=3.6, center=[0.0, 0.0, 0.0])
    fake_sphere = Geometry(
        shape="Sphere",
        radius=1.0,
    )
    fake_cylinder = Geometry(
        shape="Cylinder",
        radius=1.5,
        height=5.0,
    )

    # Material
    sample_material = Material(chemicalFormula="(Li7)2-C-H4-N-Cl6", massDensity=4.4, packingFraction=0.9)

    fake_material = Material(
        packingFraction=0.3,
        massDensity=1.0,
        chemicalFormula="V B",
    )

    # Atom
    silicon_atom = Atom(symbol="Si", coordinates=[0.125, 0.125, 0.125], siteOccupationFactor=1.0)
    vanadium_atom = Atom(
        symbol="V",
        coordinates=[0, 0, 0],
        siteOccupationFactor=0.5,
    )
    boron_atom = Atom(
        symbol="B",
        coordinates=[0, 1, 0],
        siteOccupationFactor=1.0,
    )

    # Crystallography
    sample_xtal = Crystallography(
        cifFile=Resource.getPath("inputs/crystalInfo/example.cif"),
        spaceGroup="F d -3 m",
        latticeParameters=[5.43159, 5.43159, 5.43159, 90.0, 90.0, 90.0],
        atoms=[silicon_atom, silicon_atom, silicon_atom],
    )
    fake_xtal = Crystallography(
        cifFile=Resource.getPath("inputs/crystalInfo/example.cif"),
        spaceGroup="I m -3 m",
        latticeParameters=[1, 2, 3, 4, 5, 6],
        atoms=[vanadium_atom, boron_atom],
    )

    fake_sphere_sample = CalibrantSample(
        name="fake sphere sample",
        unique_id="123fakest",
        geometry=fake_sphere,
        material=fake_material,
        crystallography=fake_xtal,
        peakIntensityFractionThreshold=0.01,
    )
    fake_cylinder_sample = CalibrantSample(
        name="fake cylinder sample",
        unique_id="435elmst",
        geometry=fake_cylinder,
        material=fake_material,
        crystallography=fake_xtal,
        peakIntensityFractionThreshold=0.01,
    )
    sample_calibrant_sample = CalibrantSample(
        name="NIST_640D",
        unique_id="001",
        geometry=sample_geometry_cylinder,
        material=sample_material,
        crystallography=sample_xtal,
        peakIntensityFractionThreshold=0.01,
    )
