Normalization For Dummies!
==========================

Requirements
------------
- :term:`Run Number`
- :term:`Background Run Number`
- :term:`State Folder` has been Initialized (see `../state/README.md`)
- :term:`Calibrant Samples` exist in the Config['instrument.calibration.sample.home'] directory. (see `../calibrant/README.md`)
- :term:`Pixel Groupings <Pixel Grouping>` exist in the State Folder associated with the Run Number (see `../pixel_grouping/README.md`)\

Steps
-----
The high-level steps are:
1. Trigger Normalization Creation
2. Assess Normalization Quality & Tweak Parameters as Needed
4. Save Normalization Data

### Trigger Normalization Creation
#### Inputs
The user provides configuration details such as run number, background run number, the calibrant sample, and the pixel grouping which eventually gets
passed to the NormalizationService.
Using the provided inputs, the application queries the :term:`Data Component` for the correct experiment data and configurations.
The :term:`Data Component` looks up the :term:`IPTS` of the run, and loads the associated experiment data.
It then converts the experiment data into :term:`Lite Mode`
It then packages and sends the :term:`Ingredients` to be processed by the :term:`Recipe Component`.
This recipe component executes a series of operations on the input ingredients. This includes:

1. Vanadium Correction
2. Spectra Focussing
3. Peak Smoothing

### Outputs
The output of trigger normalization creation is a dictionary of workspaces including processed data containing vanadium corrected, smoothed and
focused spectra. This dictionary is returned to the calling component for further processing and assessment.

### Assess Normalization Quality & Tweak Parameters as Needed
#### Inputs
The user is presented with the processed data from the previous step to assess normalization quality.
The associated view within SNAPRed depicts interactive plots that allow for visualization of the processed data.
These plots are presented in units of d-spacing (x axis) and counts (y axis).
Along with these plots, this view includes non-editable fields to display and retain information such as the run number, background run number, and
calibrant sample.
Further refinement of the data is facilitated by additional editable UI components. These include fields for dMin, dMax, intensity threshold,
smoothing level and a dropdown list containing different pixel grouping defintions.
Once these parameters are modified, the user can trigger a recalculation of the normalization by clicking the 'Recalculate' button.
This recalculation is handled asynchronously by populating specific requests with the updated parameters which yield in specific backend operations
with respect to the particular parameters modified by the user.
This allows for optimized processing and fast feedback within the UI. Once the requested operation is completed, the view is updated with the new
results.
This refinement step can be repeatedely triggered until the user is satisfied with the normalization quality.

#### Outputs
Once the user is satisfied with the quality of the normalization, a service request is made to populate a record object
to retain the final values selected by the user. These values include: run number, background run number, smoothing parameter,
the associated calibration (if one exists), and dMin value. This metadata is retained for comprehensive documentation of the normalization process.

#### Save Normalization Data
### Inputs
The record object produced by the previous step is passed to this last step. The associated view consists of text fields. These fields include
(non-editable) run number, background run number, (editable) comments, author, and version.
The user provides these details within the appropriate fields. This information is sent to initialize another service which initializes an index
entry object. The metadata included within this object includes the normalization record, the workspaces processed, and the aforementioned user
entered text.

### Outputs
SNAPRed persists this information to disk within a formated json file called "NormalizationIndex.json".
The storage location for this data is determined by the path hierarchy specified in the application.yml file, influenced by the processed run number
and a version identifier that distinguishes between different processing instances of the same dataset associated with a particular run executed at
various times.

Example Storage Path:

`SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf/normalization/NormalizationIndex.json`

This path provides a clear breakdown of how data is organized:

- SNS/SNAP/shared/Calibration/Powder: Indicates the location within the shared calibration data for powder samples.

- 04bd2c53f6bf: This segment is a unique identifier (a hash) representing the processed run number or a specific dataset version. It ensures
  that each dataset's storage location is unique, preventing data overlap and making it easier to reference specific datasets.

- normalization/NormalizationIndex.json: Specifies the type of data stored — in this case, normalization data — and the file containing the index of
  normalization records.
