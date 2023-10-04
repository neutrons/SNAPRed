## This script is to test EWM 2350:
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=2350
# This is testing that:
#   1. Rebin can work with positive binwidth and "Logarithmic" bin mode
#   2. CalculateDIFC will work with offset mode set to "Signed"
#   3. ConvertDiffCal will work with signed offsets, with offset mdoe set to "Signed"
# It is Malcolm's original prototype, but edited with the new mantid functions of this EWM, and ending when functions are tested


# script to implement new CC group functionality and test different diff calibration approaches
from mantid.simpleapi import *
from mantid.api import ITableWorkspace

import matplotlib.pyplot as plt
import numpy as np
import json
import time 
import os
import importlib
sys.path.append('/SNS/SNAP/shared/Malcolm/code/SimpleCalibrationScripts/')
import SNAPTools as snp
importlib.reload(snp)

diagnose = False #switch on get diagnostics during execution

#Set up calibration here:

#Classic DAC:
run = 57514
calibrantCif = '/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif'

isLite = True
groupingList=['Column']

#define state
stateFolder = '/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/'

#_/_/_/DON'T EDIT BELOW_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/

stateInitFilename = stateFolder + 'CalibrationParameters.json'
#get useful parameters for instrument and state
with open(stateInitFilename, "r") as json_file:
  sPrm = json.load(json_file) 

#get useful parametrers for run
SIPTS = GetIPTS(RunNumber=run,Instrument='SNAP')
        
#get raw calibration data (creating lite file if requested but it doesn't exist)
sharedDir = f'{SIPTS}shared/'

if isLite:
    extn = '.lite.nxs.h5' #changed from 'nxs' to 'nxs.h5' in later runs
    snp.makeLite(f'{SIPTS}/nexus/SNAP_{run}.nxs.h5',
        f'{SIPTS}/shared/lite/SNAP_{run}.lite.nxs.h5')
    dataDir = f'{sharedDir}lite/' #changed from 'data' to 'nexus' at some point
else:
    extn = '.nxs.h5' #changed from 'nxs' to 'nxs.h5' in later runs
    dataDir = f'{SIPTS}nexus/' #changed from 'data' to 'nexus' at some point

rawRunDataFile = f"{dataDir}nxs{str(run)}{extn}"

TOFMin = sPrm['instrumentState']['particleBounds']['tof']['minimum']
TOFMax = sPrm['instrumentState']['particleBounds']['tof']['maximum']
TOFBin = 0.001 # THIS IS POSITIVE BECAUSE WE FIXED IT!
TOFParams = (TOFMin, TOFBin, TOFMax)

#get raw data, if it isn't already available
if mtd.doesExist(f'_TOF_{run}_raw'):
    print('Nexus data already loaded')
else:
    LoadEventNexus(
        Filename=f'{dataDir}SNAP_{run}{extn}',
        OutputWorkspace=f'_TOF_{run}_raw',
        Precount='1', 
        FilterByTofMin = TOFMin,
        FilterByTofMax = TOFMax,
        LoadMonitors=False,
    )
    if isLite:
        Rebin(
            InputWorkspace=f'_TOF_{run}_raw',
            Params=TOFParams,
            OutputWorkspace=f'_TOF_{run}_raw',
            PreserveEvents=False,
            BinningMode = "Logarithmic",
        )
    else:
        Rebin(InputWorkspace=f'_TOF_{run}_raw',
            Params=TOFParams,
            OutputWorkspace=f'_TOF_{run}_raw',
            PreserveEvents=True,
            BinningMode = "Logarithmic",
        )

    
ConvertUnits(
    InputWorkspace=f'_TOF_{run}_raw',
    OutputWorkspace=f'_DSP_{run}_raw',
    Target='dSpacing',
)
        


for group in groupingList:
    
    #read grouping files (annoyingly two different formats currently)
    if isLite:
        groupingFile=f'/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_{group}.lite.nxs'
        LoadNexusProcessed(Filename=groupingFile,
            OutputWorkspace=f'FocGrp_{group}_lite')
        ws = mtd[f'FocGrp_{group}_lite']
    else:
        groupingFile=f'/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_{group}.xml'
        LoadDetectorsGroupingFile(InputFile = groupingFile,
            OutputWorkspace=f'FocGrp_{group}')
        ws = mtd[f'FocGrp_{group}']

#collect info related to grouping workspaces.
    GroupIDs = ws.getGroupIDs()

    #instantiate grouping workspace
    snp.instantiateGroupingWS(sPrm,ws,isLite)
    
    #get groupingspecific parameters
    gPrm = snp.initGroupingParams(sPrm,ws,isLite)

    #not sure of effect of binning on cross-correlate, here truncate
    #to region of dsp that is common to all subGroups
    overallDMin = max(gPrm["dMin"])
    overallDMax = min(gPrm["dMax"])
    dBin = 0.001 ## dBin IS POSITIVE BECAUSE WE FIXED IT
    DSPParams = (overallDMin, dBin, overallDMax)
    
    Rebin(
        InputWorkspace=f'_DSP_{run}_raw',
        Params=DSPParams,
        OutputWorkspace=f'_DSP_{run}_raw_reb',
        BinningMode="Logarithmic",
    )
            
    #load cif file, calculate list of d-spacings, purge weak reflections
    calibPeakList = snp.peakPosFromCif(
        cifPath=calibrantCif,
        Ithresh=0.001,
        dMin=overallDMin,
        dMax=overallDMax,
        verbose=False,
    )
    
    #ensure peaks are sorted in ascending order
    calibPeakListSorted = np.array(sorted(calibPeakList, key= float))
    
    print(calibPeakListSorted)

    #process each group separately
    for subGroup in GroupIDs:
        subGroupIDs = ws.getDetectorIDsOfGroup(int(subGroup))
        nSpectraInSubGroup = len(subGroupIDs)
        
        #NOMAD script uses brightest spectrum as reference, but this will be pray to randomness
        #I prefer to choose the central pixel of each group. This should be the equatorial pixel
        #with 2theta closest to the average 2theta.
        #
        #For now, just choose median value...
        refID = int(np.median(subGroupIDs))
        fwhm = max(calibPeakListSorted)*gPrm["delDOverD"][int(subGroup)-1]*2.35
        
        print(f'''
        Group: {int(subGroup)}, 
        Central 2theta: {gPrm["twoTheta"][int(subGroup)-1]:.3f}
        Number of Spectra: {nSpectraInSubGroup}
        Reference pixel: {refID}
        Longest d-spacing: {max(calibPeakListSorted):.3f}
        del_d over d: {gPrm["delDOverD"][int(subGroup)-1]:.3f}
        Estimate FWHM: {fwhm:.3f}
        ''')
                    
        #MaxDSpaceShift has to be determined for each group
        MDSh = fwhm*2.5
        
        CrossCorrelate(
            InputWorkspace=f'_DSP_{run}_raw_reb',
            OutputWorkspace=f'CC_{run}_subGroup{int(subGroup)}',
            ReferenceSpectra=refID,
            WorkspaceIndexList=subGroupIDs,
            XMin = DSPParams[0],
            XMax = DSPParams[2],
            MaxDSpaceShift=MDSh,
        )   
        GetDetectorOffsets(
            InputWorkspace=f'CC_{run}_subGroup{int(subGroup)}',
            OutputWorkspace=f'Off_{run}_subGroup{int(subGroup)}',
            Step = 0.001,
            # DReference=2.0,
            XMin = -100,
            XMax = 100,
            OffsetMode='Signed',
        )            
            
        print('limits:',-(MDSh/dBin),' to ',(MDSh/dBin))

    #Create single offsets ws with all groups
    
    if len(GroupIDs) == 1:
        RenameWorkspace(
            InputWorkspace=f'Off_{run}_subGroup1',
            OutputWorkspace=f'Off_{run}',
        )
    elif len(GroupIDs) == 2:
        Plus(
            LHSWorkspace=f'Off_{run}_subGroup1',
            RHSWorkspace=f'Off_{run}_subGroup2',
            OutputWorkspace=f'Off_{run}',
        )
    else:
        Plus(
            LHSWorkspace=f'Off_{run}_subGroup1',
            RHSWorkspace=f'Off_{run}_subGroup2',
            OutputWorkspace=f'Off_{run}',
        )
        for subGroup in range(3,len(GroupIDs)+1):
            Plus(
                LHSWorkspace=f'Off_{run}',
                RHSWorkspace=f'Off_{run}_subGroup{subGroup}',
                OutputWorkspace=f'Off_{run}',
            )
    
    if diagnose:
        ConvertSpectrumAxis(
            InputWorkspace=f'Off_{run}',
            OutputWorkspace=f'Off_vs_tt_{run}',
            Target='Theta',
            EMode='Direct',
            OrderAxis=True,
        )
            
    if not diagnose:    
        for subGroup in GroupIDs:
            DeleteWorkspace(Workspace=f'Off_{run}_subGroup{subGroup}')
            DeleteWorkspace(Workspace=f'CC_{run}_subGroup{int(subGroup)}')
       
    #get offsets
    offws = mtd[f'Off_{run}']
    offsets = offws.extractY().ravel()
    #calculate DIFCs
    CalculateDIFC(
        InputWorkspace=f'_TOF_{run}_raw',
        OutputWorkspace=f'_{run}_difcs',
        OffsetMode = "Signed",
        BinWidth = TOFBin,
    )
    
    ConvertDiffCal(
        OffsetsWorkspace = f'Off_{run}',
        OutputWorkspace = "tableWS",
        OffsetMode = "Signed",
        BinWidth = TOFBin,
    )    
    #Apply offset correction to input workspace

    CloneWorkspace(
        InputWorkspace=f'_TOF_{run}_raw',
        OutputWorkspace=f'_TOF_{run}_cal',
    )
    
    ApplyDiffCal(
        InstrumentWorkspace=f'_TOF_{run}_cal',
        CalibrationWorkspace='tableWS',
    )
        
    ConvertUnits(
        InputWorkspace=f'_TOF_{run}_cal',
        OutputWorkspace=f'_DSP_{run}_cal',
        Target='dSpacing',
    )
        
    Rebin(
        InputWorkspace=f'_DSP_{run}_cal',
        Params=DSPParams,
        OutputWorkspace=f'_DSP_{run}_cal_reb',
        BinningMode = "Logarithmic",
    )


             
   