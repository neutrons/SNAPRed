from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "CalculateOffsetAlgorithm"


class CalculateOffsetAlgorithmAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ExtractionIngredients", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients):
        self.dBin = ingredients.dBin
        ipts = ingredients.runConfig.IPTS
        runNumber = ingredients.runConfig.runNumber
        rawDataPath = ingredients.rawDataPath
        groupingFile = ingredients.groupingFile
        dataDir = ingredients.dataDir

        with open(stateInitFilename, "r") as json_file:
            self.sPrm = json.load(json_file) 

        getDSpacingWS(inputWStof, inputWSdsp)

        # focused grouping ws
        focusWSname = '_{runNumber}_FocGroup'
        ext = 
        if ext=='nxs':
            LoadNexusProcessed(
                Filename=groupingFile,
                OutputWorkspace=focusWSname,
            )
        elif ext=='xml':
            LoadDetectorsGroupingFile(
                InputFile = groupingFile,
                OutputWorkspace=focusWSname,
            )
        else:
            throw RuntimeError("invalid file extension for groupingFile")
        self.focusWS = mtd[focusWSname]
        self.GroupIDs = self.focusWS.getGroupIDs()

        #instantiate grouping workspace
        snp.instantiateGroupingWS(sPrm,ws,isLite)
        #get groupingspecific parameters
        self.gPrm = snp.initGroupingParams(sPrm,ws,isLite)

        self.overallDMin = max(gPrm["dMin"])
        self.overallDmax = min(gPrm["dMax"])
        self.dBin = -0.001

        inputWS = f'_DSP_{run}_raw'
        rebinWS = f'_DSP_{run}_raw_reb'
        self.mantidSnapper.Rebin(
            InputWorkspace=inputWS,
            Params=f'{overallDMin},{dBin},{overallDMax}',
            OutputWorkspace=rebinWS,
        )

    def calculateGroupOffset(self, inputWS, subGroupIDs, outputWS):
        # inputWS is in d-space with logarithmic binning

        #For now, just choose median value...
        refID = getRefID(subGroupIDs)
            
        wscc = 'CC_'+outputWS
        self.mantidSnapper.CrossCorrelate(
            "Cross-Correlating spectra",
            InputWorkspace=inputWS,
            OutputWorkspace=wscc,
            ReferenceSpectra=refID,
            WorkspaceIndexList=subGroupIDs,
            XMin = Xmin,
            XMax = Xmax,
            MaxDSpaceShift=MDSh,
        )
        offsetWS = self.mantidSnapper.GetDetectorOffsets(
            "Calculate offsets for pixels",
            InputWorkspace=wscc,
            OutputWorkspace=outputWS,
            Step = abs(self.dBin),
            XMin = -100,
            XMax = 100,
            OffsetMode='Signed',           
        )
        self.mantidSnapper.DeleteWorkspace(
            "removing temp cross-correlate workspace",
            Worksheet = wscc,
        )
        self.mantidSnapper.executeQueue()
        return offsetWS

    def sumWorksheetList(self, wsList: List[str], outputWS: str) -> str:
        self.mantidSnapper.RenameWorkspace(
            "Beginning sum of workspaces... {wsList[0]}",
            InputWorkspace=wsList[0],
            OutputWorkspace=outputWS,
        )
        for ws in wsList[1:]:
            self.mantidSnapper.Plus(
                "... adding {ws}",
                LHSWorkspace=outputWS,
                RHSWorkspace=ws,
                OutputWorkspace=outputWS,
            )
            self.mantidSnapper.DeleteWorkspace(
                "... removing {ws}",
                Workspace = ws,
            )
        self.mantidSnapper.executeQueue()
        return outputWS

    def convertDifCalLog(self, inputWS, offsetWS, outputWS):
        #calculate DIFCs
        tempDIFCsWS = f'_{self.runNumber}_difcs'
        self.mantidSnapper.CalculateDIFC(
            "Find DIFC based on instrument definition",
            InputWorkspace=inputWS,
            OutputWorkspace=tempDIFCsWS,
        )
        outws = self.mantidSnapper.ConvertDifCalLog(
            "Correct DIFC values with log offsets, binning {self.dBin}",
            OffsetsWorkspace=offsetWS,
            PreviousCalibration=tempDIFCsWS,
            BinWidth=self.dBin,
            OutputWorkspace=outputWS,
        )
        self.mantidSnapper.DeleteWorkspace(
            "Delete temporary workspace {tempDIFCsWS}",
            tempDIFCsWS,
        )
        self.mantidSnapper.executeQueue()
        return outws

    def applyCalibrationCorrect(self, inputWS, calibrationWS, correctedWS):
        #Apply offset correction to input workspace
        CloneWorkspace(
            InputWorkspace=inputWS,
            OutputWorkspace=correctedWS,
        )
        ApplyDiffCal(
            InstrumentWorkspace=correctedWS,
            CalibrationWorkspace=calibrationWS
        )  
        ConvertUnits(
            InputWorkspace=correctedWS,
            OutputWorkspace=correctedWS,
            Target='dSpacing'
        )       
        Rebin(
            InputWorkspace=correctedWS,
            Params=f'{overallDMin},{self.dBin},{overallDMax}',
            OutputWorkspace=correctedWS
        )

    def rerunAlgo(self, inputWS):
        # inputWS is a worksheet in d-space with logarithmic binning
        # for each group separately, find needed offsets
        offsetWSs = []
        for subGroup in self.GroupIDs:
            subGroupIDs = getSubgroupIDs(subGroup)
            nSpectraInSubGroup = len(subGroupIDs)
            MDSh = getMDSh(calibPeakListSorted)
            
            wsname = f'{run}_subGroup{int(subGroup)}'
            calculateGroupOffset(subGroupIDs, wsname)
            offsetWSs.append(wsname)

        # create single offsets ws with all groups
        totalOffsetWS = f'offsets_{run}'
        sumWorksheetList(offsetWSs, totalOffsetWS)
        # maximum offset should converge to 0 with several iterations
        data["maxOffset"] = np.max(mtd[totalOffsetWS].extractY().ravel())

        #get difcal corrected by offsets
        difcalWS = "_{run}_difcal"
        self.convertDifCalLog(rebinWS)
        #Apply difcal to input workspace
        correctedWS = f'_DSP_{run}_cal_reb'
        applyCalibrationCorrect(inputWStof, difcalWS, correctedWS)
        return data


    def PyExec(self):
        # run the algo
        self.log().notice("Execution of extraction of calibration constants START!")

        # get the ingredients
        ingredients = ExtractionIngredients(
            **json.loads(self.getProperty("ExtractionIngredients").value)
        )
        self.chopIngredients(ingredients)
        
        return self.rerunAlgo(self.rebinWS)

# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalculateOffsetAlgorithm)
