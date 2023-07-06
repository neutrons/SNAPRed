from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "OffsetCalibrationAlgorithm"


class OffsetCalibrationAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ExtractionIngredients", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)



    def PyExec(self):
        # run the algo
        self.log().notice("Execution of extraction of calibration constants START!")

        # get the ingredients
        ingredients = ExtractionIngredients(
            **json.loads(self.getProperty("ExtractionIngredients").value)
        )

        ipts = ingredients.runConfig.IPTS
        runNumber = ingredients.runConfig.runNumber
        rawDataPath = ingredients.rawDataPath
        groupingFile = ingredients.groupingFile
        dataDir = ingredients.dataDir

        sPrm = openStateFile(stateInitFilename)
        getDSpacingWS(inputWStof, sPrm, isLite, inputWSdsp)
        

        for group in groupingList:
            #Move on to next step of PD calibration.
            DiffractionFocussing(
                InputWorkspace=correctedWS,
                GroupingWorkspace=focusWS.name(),
                OutputWorkspace=f'_DSP_{run}_cal_{group}'
            )
                
            ConvertUnits(
                InputWorkspace=f'_DSP_{run}_cal_{group}',
                OutputWorkspace=f'_DSP_{run}_cal_{group}',
                Target='TOF'
            )
                    
            # PDCalibration executed group by group
            
        
            for subGroup in GroupIDs:
            
                subGroupIDs = focusWS.getDetectorIDsOfGroup(int(subGroup))
                nSpectraInSubGroup = len(subGroupIDs)
                fwhm = max(calibPeakListSorted)*gPrm["delDOverD"][int(subGroup)-1]*2.35
                
                #load get d-spacings, apply group d-limits and purge weak reflections
                calibPeakList = snp.peakPosFromCif(cifPath=calibrantCif,
                    Ithresh=0.01,
                    dMin=gPrm["dMin"][subGroup-1],
                    dMax=gPrm["dMax"][subGroup-1],
                    verbose=False)
                    
                calibPeakListSorted = np.array(sorted(calibPeakList, key= float))
                
                print(f'''      
                print('PDCalib')
                Group: {int(subGroup)}, 
                Central 2theta: {gPrm["twoTheta"][int(subGroup)-1]:.3f}
                Number of Spectra: {nSpectraInSubGroup}
                D-Spacings to be fitted: {max(calibPeakListSorted):.3f}
                del_d over d: {gPrm["delDOverD"][int(subGroup)-1]:.3f}
                Estimate FWHM: {fwhm:.3f}
                ''')
                            
                #extract spectrum to fitfunctions
                ExtractSpectra(InputWorkspace=f'_DSP_{run}_cal_{group}',
                    # XMin = gPrm["dMin"][subGroup-1],
                    # XMax = gPrm["dMax"][subGroup-1],
                    StartWorkspaceIndex=int(subGroup)-1, 
                    EndWorkspaceIndex=int(subGroup)-1,
                    outputWorkspace=f'_DSP_{run}_cal_subGroup{subGroup}')
                    
                peaks,peakBoundaries = snp.removeOverlappingPeaks(calibrantCif,
                    sPrm,
                    gPrm,
                    subGroup)
                
                
                TOFMin, TOFBin, TOFMax = getTOFBinningParams(sPrm)
                #run PDCalibration on extracted ws
                PDCalibration(
                    InputWorkspace=f'_DSP_{run}_cal_subGroup{subGroup}',
                    TofBinning=f'{TOFMin},{TOFBin},{TOFMax}',
                    PeakFunction='Gaussian',
                    BackgroundType='Linear',
                    PeakPositions=peaks,
                    PeakWindow=peakBoundaries,
                    CalibrationParameters='DIFC',
                    OutputCalibrationTable=f'_PDCal_table_{subGroup}',
                    DiagnosticWorkspaces=f'_PDCal_diag_{subGroup}'
                )
            

# Register algorithm with Mantid
AlgorithmFactory.subscribe(ExtractionAlgorithm)
