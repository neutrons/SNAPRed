## This script is to test EWM 2350:
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=2350
#  It is Malcolm's original prototype, but edited with the new mantid functions of this EWM


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

#Chunrou clamp cell:
# run = 58469
# calibrantCif = '/SNS/SNAP/shared/Calibration/CalibrantSamples/EntryWithCollCode94254_LaB6_neutron_2001.cif'
# 
isLite = True
groupingList=['Column']

#define state
stateFolder = '/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/'

#_/_/_/DON'T EDIT BELOW_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/_/

def initGroupingParams(sPrm,gpWS,isLite):

  import math

  #grouping WS should be instantiate before calling this function (i.e. instrument is loaded
  #corresponding to the specific state
   
  band = sPrm["instrumentState"]["instrumentConfig"]["bandwidth"]
  L1 = sPrm["instrumentState"]["instrumentConfig"]["L1"]
  L2 = sPrm["instrumentState"]["instrumentConfig"]["L2"]
  L=L1+L2
  delToT = sPrm["instrumentState"]["instrumentConfig"]["delTOverT"]
  delLoL = sPrm["instrumentState"]["instrumentConfig"]["delLOverL"]
  if sPrm["instrumentState"]["detectorState"]["guideStat"] == 1:
     delTh = sPrm["instrumentState"]["instrumentConfig"]["delThWithGuide"]
  else:
     delTh = sPrm["instrumentState"]["instrumentConfig"]["delThNoGuide"]
    
  TOFMin = sPrm['instrumentState']['particleBounds']['tof']['minimum']
  TOFMax = sPrm['instrumentState']['particleBounds']['tof']['maximum']

  lamMin = sPrm['instrumentState']['particleBounds']['wavelength']['minimum']
  lamMax = sPrm['instrumentState']['particleBounds']['wavelength']['maximum']

  det_arc1 = sPrm["instrumentState"]["detectorState"]["arc"][0]
  det_arc2 = sPrm["instrumentState"]["detectorState"]["arc"][1]
  det_lin1 = sPrm["instrumentState"]["detectorState"]["lin"][0]
  det_lin2 = sPrm["instrumentState"]["detectorState"]["lin"][0]
  

  GroupDetectors(InputWorkspace=gpWS,
              OutputWorkspace='groupedWS',
              Behaviour='Average',
              CopyGroupingFromWorkspace=gpWS)
  
  #Generate a dictionary with important diffraction parameters
  groupedws = mtd['groupedWS']
  dMin = []
  dMax = []
  tTheta = []
  delD = []
  verb = False

  ungroupedWS = gpWS
  GroupIDs = ungroupedWS.getGroupIDs()
  if verb:
     print('InitGroupingParams Output...')


  groupingws = gpWS
  if True:
    for groupID in GroupIDs:
      #get limiting tthetas in group

      group2Thetas = []
      pixInGroup = ungroupedWS.getDetectorIDsOfGroup(int(groupID))
      # print(f'group: {groupID}')
      # print(pixInGroup)
      for pix in pixInGroup:
        group2Thetas.append(ungroupedWS.spectrumInfo().twoTheta(int(pix)))
      
      # print(f'min 2theta: {groupMin2Theta*180/np.pi:.2f} max 2theta: {groupMax2Theta*180/np.pi:.2f}')

      specInfo = gpWS.spectrumInfo()
      tTheta.append(specInfo.twoTheta(int(groupID)-1))
      
      
      groupMin2Theta = min(group2Thetas)
      groupMax2Theta = max(group2Thetas)
      dMin.append(3.9561e-3*(1/(2*np.sin(groupMax2Theta/2)))*TOFMin/L)
      dMax.append( 3.9561e-3*(1/(2*np.sin(groupMin2Theta/2)))*TOFMax/L)
      delD.append(snp.delDoD(delToT,delLoL,delTh,specInfo.twoTheta(int(groupID)-1)))


  FocGroupParm = {"lamMin":lamMin,
                  "lamMax":lamMax,
                  "TOFMin":TOFMin,
                  "TOFMax":TOFMax,
                  "twoTheta":tTheta,
                  "groupMinTwoTheta":groupMin2Theta,
                  "groupMaxTwoTheta":groupMax2Theta,
                  "dMin":dMin,
                  "dMax":dMax,
                  "delDOverD":delD}

  return FocGroupParm



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
    gPrm = initGroupingParams(sPrm,ws,isLite)

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
            XMin = overallDMin,
            XMax = overallDMax,
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
            
    #######################################################################    
    #Diagnostic: calculate offsets for corrected data to see if they improve
    #######################################################################
    if diagnose:

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
                InputWorkspace=f'_DSP_{run}_cal_reb',
                OutputWorkspace=f'CC_{run}_subGroup{int(subGroup)}_cal',
                ReferenceSpectra=refID,
                WorkspaceIndexList=subGroupIDs,
                XMin = overallDMin,
                XMax = overallDMax,
                MaxDSpaceShift=MDSh,
            )
            GetDetectorOffsets(
                InputWorkspace=f'CC_{run}_subGroup{int(subGroup)}_cal',
                OutputWorkspace=f'Off_{run}_subGroup{int(subGroup)}_cal',
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
                InputWorkspace=f'Off_{run}_subGroup1_cal',
                OutputWorkspace=f'Off_{run}_cal',
            )
        elif len(GroupIDs) == 2:
            Plus(
                LHSWorkspace=f'Off_{run}_subGroup1_cal',
                RHSWorkspace=f'Off_{run}_subGroup2_cal',
                OutputWorkspace=f'Off_{run}_cal',
            )
        else:
            Plus(
                LHSWorkspace=f'Off_{run}_subGroup1_cal',
                RHSWorkspace=f'Off_{run}_subGroup2_cal',
                OutputWorkspace=f'Off_{run}_cal',
            )
            for subGroup in range(3,len(GroupIDs)+1):
                Plus(
                    LHSWorkspace=f'Off_{run}_cal',
                    RHSWorkspace=f'Off_{run}_subGroup{subGroup}_cal',
                    OutputWorkspace=f'Off_{run}_cal',
                )
    
    #Move on to next step of PD calibration.
    
    if isLite:
        DiffractionFocussing(
            InputWorkspace=f'_DSP_{run}_cal',
            GroupingWorkspace=f'FocGrp_{group}_lite',
            OutputWorkspace=f'_DSP_{run}_cal_{group}',
        )
    else:
        DiffractionFocussing(
            InputWorkspace=f'_DSP_{run}_cal',
            GroupingWorkspace=f'FocGrp_{group}',
            OutputWorkspace=f'_DSP_{run}_cal_{group}',
        )
            
    ConvertUnits(
        InputWorkspace=f'_DSP_{run}_cal_CC_{group}',
        OutputWorkspace=f'_TOF_{run}_cal_CC_{group}',
        Target='TOF',
    )
            
    # PDCalibration executed group by group, so need handle on focused ws
    ws_foc = mtd[f'_TOF_{run}_cal_CC_{group}']
    
    #Before running PDCalibration get original DIFC values for group
    CreateDetectorTable(
        InputWorkspace=f'_TOF_{run}_cal_CC_{group}',
        DetectorTableWorkspace=f'_DetTable_{run}_cal_CC_{group}_CC',
    )
    
        
    #Create new cal workspace that will be modified after PDCalibration
    #create diffCal workspace with new values.
    # TODO pending EWM1851 replace all CALC_CC_PD operations with CombineDiffCal
    CAL_CC_PD = CreateEmptyTableWorkspace()
    CAL_CC_PD.addColumn(type="int",name="detid",plottype=6)
    CAL_CC_PD.addColumn(type="float",name="difc",plottype=6)
    CAL_CC_PD.addColumn(type="float",name="difa",plottype=6)
    CAL_CC_PD.addColumn(type="float",name="tzero",plottype=6)
    CAL_CC_PD.addColumn(type="float",name="tofmin",plottype=6)
    CAL_CC_PD.setLinkedYCol(0, 1)
    
    #get handle to original difc
    origCal = mtd['CAL_CC'].toDict() 
        
    for subGroup in GroupIDs:
        
        subGroupIDs = ws.getDetectorIDsOfGroup(int(subGroup))
        nSpectraInSubGroup = len(subGroupIDs)
        fwhm = max(calibPeakListSorted)*gPrm["delDOverD"][int(subGroup)-1]*2.35
        
        #load get d-spacings, apply group d-limits and purge weak reflections
        calibPeakList = snp.peakPosFromCif(
            cifPath=calibrantCif,
            Ithresh=0.01,
            dMin=gPrm["dMin"][subGroup-1],
            dMax=gPrm["dMax"][subGroup-1],
            verbose=False,
        )
            
        calibPeakListSorted = np.array(sorted(calibPeakList, key= float))
        
        #getDIFC
        difc = ws_foc.spectrumInfo().difcUncalibrated(int(subGroup-1))
        
        print(f'''
        
        PDCalib
        Group: {int(subGroup)}, 
        Central 2theta: {gPrm["twoTheta"][int(subGroup)-1]:.3f}
        Central DIFC: {difc:.3f}
        Number of Spectra: {nSpectraInSubGroup}
        D-Spacings to be fitted: {max(calibPeakListSorted):.3f}
        del_d over d: {gPrm["delDOverD"][int(subGroup)-1]:.3f}
        Estimate FWHM: {fwhm:.3f}
        ''')
                    
        #extract spectrum to fitfunctions
        ExtractSpectra(
            InputWorkspace=f'_TOF_{run}_cal_CC_{group}',
            # XMin = gPrm["dMin"][subGroup-1],
            # XMax = gPrm["dMax"][subGroup-1],
            StartWorkspaceIndex=int(subGroup)-1,
            EndWorkspaceIndex=int(subGroup)-1,
            outputWorkspace=f'_TOF_{run}_cal_CC_subGroup{subGroup}'
        )
                            
        peaks,peakBoundaries = snp.removeOverlappingPeaks(
            calibrantCif,
            sPrm,
            gPrm,
            subGroup,
        )
            
        for i,dsp in enumerate(peaks):
            print(f'Peak: {i} at {dsp:.3f} between {peakBoundaries[i*2]:.3f} and {peakBoundaries[i*2+1]:.3f}') 
            print(f'in tof: {dsp*difc} between {peakBoundaries[i*2]*difc} and {peakBoundaries[i*2+1]*difc}') 
        #run PDCalibration on extracted ws
        PDCalibration(
            InputWorkspace=f'_TOF_{run}_cal_CC_subGroup{subGroup}',
            TofBinning=TOFParams,
            PeakFunction='Gaussian',
            BackgroundType='Linear',
            PeakPositions=peaks,
            PeakWindow=peakBoundaries,
            CalibrationParameters='DIFC',
            HighBackground=True,
            OutputCalibrationTable=f'_PDCal_table_{subGroup}',
            DiagnosticWorkspaces=f'_PDCal_diag_{subGroup}',
        )
        
        #OutputCalibrationTable contrains values for full instrument, but only those with
        #pixels in the group have the corrected DIFC.
        table = mtd[f'_PDCal_table_{subGroup}'].toDict()
        DIFC_PD = table['difc'][subGroupIDs[0]] # take from first pixel in group
        
        table = mtd[f'_DetTable_{run}_cal_CC_{group}_CC'].toDict()
        DIFC_CC = table['DIFC'][int(subGroup)-1]
        
        #correction factor is ratio of group CC DIFC and group PD DIFC
        PDFactor = DIFC_PD/DIFC_CC
        print('PDFactor: ',PDFactor)
                    
                    
        for i in subGroupIDs:
            nextRow = { 'detid': i,
                'difc': origCal['difc'][i]*PDFactor,
                'difa': 0,
                'tzero': 0,
                'tofmin': 0 }
            CAL_CC_PD.addRow( nextRow )
    
        #Clean up if not diagnose
        #TODO
        
        #apply calibration to inspect results
        
        CloneWorkspace(
            InputWorkspace=f'_TOF_{run}_raw',
            OutputWorkspace= f'_TOF_{run}_cal_CC_PD',
        )
        
        ApplyDiffCal(
            InstrumentWorkspace=f'_TOF_{run}_cal_CC_PD',
            CalibrationWorkspace='CAL_CC_PD',
        )
            
        ConvertUnits(
            Inputworkspace=f'_TOF_{run}_cal_CC_PD',
            OutputWorkspace=f'_DSP_{run}_cal_CC_PD',
            Target='dSpacing',
        )
            
        if isLite:
            DiffractionFocussing(
                InputWorkspace=f'_DSP_{run}_cal_CC_PD',
                GroupingWorkspace=f'FocGrp_{group}_lite',
                OutputWorkspace=f'_DSP_{run}_cal_CC_PD_{group}',
            )
        else:
            DiffractionFocussing(
                InputWorkspace=f'_DSP_{run}_cal_CC_PD',
                GroupingWorkspace=f'FocGrp_{group}',
                OutputWorkspace=f'_DSP_{run}_cal_CC_PD_{group}',
            )
             
   