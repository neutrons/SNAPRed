Calibration Workflow
====================

Requirements
------------
- :term:`Run Number`
- :term:`State Folder` has been Initialized (see `../state/README.md`)
- :term:`Calibrant Samples` exist in the Config['instrument.calibration.sample.home'] directory. (see `../calibrant/README.md`)
- :term:`Pixel Grouping's <Pixel Group>` exist in the State Folder associated with the Run Number (see `../pixel_grouping/README.md`)\

Steps
-----

The high-level steps are:

1. Trigger Calibration Creation
2. Assess Calibration Quality
3. Accept or Reject Calibration
4. Iterate ...

Trigger Calibration Creation
----------------------------

The user provides configuration details such as run number, the calibrant sample, and the pixel grouping which eventually gets passed to the CalibrationService.
Using the provided inputs, the application queries the :term:`Data Component` for the correct experiment data and configurations.
The :term:`Data Component` looks up the :term:`IPTS` of the run, and loads the associated experiment data.
It then converts the experiment data into :term:`Lite Mode`
It then packages and sends the :term:`Ingredients` to be processed by the :term:`Recipe Component`.
The Calibration Recipe executes both a :term:`Pixel Calibration` and a :term:`Group Calibration` using the provided inputs. This includes:

1. Pixel Calibration
2. Group Calibration
3. (Repeat till offset value is reached.)

Assess Calibration Quality
--------------------------

The user is presented with the results of the calibration and is given the option to load previous calibrations and compare them to the current
calibration. Once the user has decided on the calibration they want to persist to disk, they can hit the continue button to continue to the
saving step.

Accept or Reject Calibration
----------------------------

The user is given the option to accept the calibration by selecting the "Continue" button.
If the user selects "Continue", the calibration is saved using the provided inputs.
The inputs indicate the author, any comments on the calibration, and which runs the calibration is valid for.
The backend will then collect all this metadata and relevant workspaces and save them under a versioned folder in the :term:`Calibration Folder`.
The backend will then also create an entry in the :term:`Calibration Index` to track the calibration.
Otherwise, the user may select "Iterate" to re-run the calibration with different inputs.
The outputs would then be renamed and preserved in the :term:`Workspace List` for comparison.
Once the :term:`User` is satisfied with the calibration, they may select select which of the iterations to save in the final save step.
The user may also select "Cancel" to reset the form at anytime,
however it is reccommended to clear all workspaces if you wish to begin again.

Example Storage Path:

`SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/diffraction/CalibrationIndex.json`

This path provides a clear breakdown of how data is organized:

- SNS/SNAP/shared/Calibration/Powder: Indicates the location within the shared calibration data for powder samples.

- 04bd2c53f6bf6754: This segment is a unique identifier (a hash) representing the processed run number or a specific dataset version. It ensures
  that each dataset's storage location is unique, preventing data overlap and making it easier to reference specific datasets.

- diffraction/CalibrationIndex.json: Specifies the type of data stored — in this case, calibration data — and the file containing the index of
  calibration records.
