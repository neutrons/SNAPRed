Glossary
========
.. TODO: Provide links to a term's page if it exists
.. glossary::

    ADS
        TODO

    Algorithm
        A set of instructions that can be executed to produce a result.
        In the context of SNAP, this mostly refers to Mantid Algorithms or collections of Mantid Algorithms triggered by a Recipe.
        Base Mantid Algorithm Index: https://docs.mantidproject.org/nightly/algorithms/categories/AlgorithmIndex.html
        There are also a collection of custom algorithms that are used to perform SNAP specific tasks in this repo.

    API Layer
        The architectural layer that provides the single point of interaction between the backend and frontend.
        Agnostic of frontend implementation, it recieves requests from the frontend and forwards them to the corresponding Service.
        In addition it is also responsible for catching errors, and returning a human readable error message to the frontend.

    Background Run Number
        The identifier for a background run, which is a measurement taken under the same conditions as a
        primary experimental run but without the sample present. This data is used to subtract background noise
        and artifacts from the experimental data, enabling more accurate analysis. In the context of SNAP, the
        Background Run ID refers to the unique ID associated with such a run, used in processes like
        :term:`Normalization` to correct the primary data set.

    Calibrant Samples
        A json configuration representing various phyiscal properties of known samples that are used to calibrate the instrument.
        Most notably, the cif file that contains the known crystallographic properties of the sample.

    Calibration
        The process by which the instrument state configuration is calibrated to account for the effects of the instrument on the diffraction data.
        This is done by comparing the diffraction data to a known standard or previous Calibration, and adjusting the instrument to match.
        This is done by a series of algorithms that are triggered by a Recipe.

    Calibration Folder
        TODO

    Calibration Index
        The data/file that contains a ledger of which Calibration applies to which Run in a given Instrument State.

    Calibration Mask
        The outcome of various systematic effects can lead to failures in pixel event detection. This may cause the calibration process to fail for individual or
        groups of pixels. These are handled by creating a mask indicating which faulty pixels will not be used in any reduction employing the related
        calibration.

    Calibration Record
        This is a file written to the :term: `state <State>` calibration folder that contains a record  of information related to the calibration.
        This includes all of the parameters that govern the calibration, the location of all the files generated during the calibration (e.g. calibration
        mask) and also some statistical information capturing the "quality" of the calibration.

    CIS
        Computational Instrument Scientist (CIS).
        A domain expert, usually responsible for all aspects of data collection and reduction using a given instrument.

    Config
        A configuration mapping used to control many configurable aspects of the SNAPRed application.
        This is implemented as a mapping between multiple-key sequences, represented as "<key-1>.<key-2>. ... <key-n>", and string and number values, such as paths or default-parameter values.
        At the start of the application, this configuration is initialized from the YAML-format file ``application.yml`` (, which may be overridden using the ``dev.yml`` file).

    Component
        Architectural term for a single unit of abstraction that fulfills a mid level Developer Requirement.
        This includes concepts like :term:`Data State Management`, Persistence, and Data Calculation

    Data Objects
        The architectural layer that represents concepts and contracts as easily validated and serializable objects.
        It consists entirely of Pydantic models with minimal to no buisness logic.
        These objects represent things like :term:`Requests <User Request>`, :term:`Focus Groups <Focus Group>`, :term:`Instrument State`, etc.

    Data Component
        The architectural Component that provides an abstraction for data aquisition and storage.
        It provides a means for the rest of the backend to interact with data without having to worry about the underlying implementation.
        It handles serialization and deserialization of data to terms the backend is familiar with.
        Example: A user may request a Calibration for a specific run, regardless of whether the data is stored on a remote server or locally,
        it will be retrieved, deserialized, and returned.

    Data State Management
        The process by which the backend manages the state of the data it is working with.
        This includes things like Caching, Serialization, Deserialization, Calculation, and Transformation.
        If given the proper interface, consumers should recieve consistently populated Data Access Objects regardless of current state.

    Diagnostic Mode
        TODO

    Detector
        A physical component of the instrument used for for particle-flux measurement, usually comprised of multiple :term:`pixels <Pixel>`.

    Detector ID
        A unique integer associated with each :term:`pixel <Pixel>` in the instrument.
        Apart from uniqueness, detector-id integer values are arbitrary.  For example, they are not required to be consecutive,
        and negative values may be used to indicate pixels with a special function, such as monitor pixels.

    Diffraction Focussing
        TODO

    Focus Group
        A predetermined set of parameters used to split diffraction data into useful formations, i.e. like slices vs. squares of pizza
        This may include predetermined data such as dimensions and tolerances, or derived values such as :term:`Pixel Grouping Parameters <Pixel Grouping Parameters>`.

    :doc:`Grouping-schema Map <developer/architecture/backend/data/GroupingMap>`
        A mapping between :term:`grouping-schema <Grouping Schema>` common names, and their file locations on disk.
        File locations may be specified either as *relative* paths, with respect to <instrument.calibration.powder.grouping.home>, or as *absolute* paths.

    Grocery
        Within SNAPRed code, this refers to workspace data (as opposed to ingredient data) which are needed for an operation.
        They are requested by handing the Grocery Service a grocery list of workspaces to fetch.

    Group Calibration
        TODO

    Grouping Schema
        A relationship, usually a mapping, between each :term:`detector-id <Detector ID>` in the instrument and an integer group number.

    HDF5
        The most recent variant of Hierarchical Data Format (HDF).  A binary file format designed to scalably store and access large scientific data sets.

    Histogram
        A graphical representation of the distribution of numerical data. It is an estimate of the probability distribution of a
        continuous variable and is used to visualize the distribution of data points. A histogram is constructed by dividing the
        range of the data into bins (also called intervals or buckets) and counting the number of data points that fall into each bin.

    Ingredients
        The data required to perform a unit of calculations and produce a well cooked result.
        These are typically the configuration input to a :term:`Recipe`.
        They are also typically passed through a Recipe to the various algorithms it triggers.

    Instrument
        The physical apparatus used to collect diffraction data. In the case of SNAP, it consists of a sample to shoot neutrons at,
        a source that provides said neutrons, and several detectors with variable physical positions, the specifics of depending on the requirements of a given experiment.
        The configuration of these components define what is referred to as an :term:`Instrument State <Instrument State>`.

    Instrument State
        The configuration of an instrument at a given point in time. This includes the positions of the detectors, the sample, and the source.
        It is also dependant on a number of other configurations relating to the instrument.

    Interface Layer
        The architectural layer that provides the single point of interaction between the backend and frontend.
        Agnostic of frontend implementation, it recieves requests from the frontend and forwards them to the Orchestration Layer.

    IPTS
        TODO

    JSON
        Javascript Object Notation (JSON).
        An text-based data representation, used by many applications where a human-editable representation is required.

    Layer
        A collection of :term:`Components <Component>` that work together to provide a single unit of high level Developer Requirements
        Examples include: API, Orchestration, Data Processing, etc.

    Lite Mode
        The SNAP instrument uses :term:`detectors<Detector>` comprised of many more :term:`pixels<Pixel>` than are actually required to achieve the target d-spacing resolution.
        In *Lite* mode, all of the event data will be used, but it will be grouped into *effective* pixels, one for each 8x8 block of *native* pixels. (See :term:`Native Mode`)

    Mantid
        Neutron scattering data reduction code maintained by the `Mantid Project <https://www.mantidproject.org/>`_.

    Mantid Snapper
        A thin wrapper around the Mantid Algorithm API that allows for meta processes to be performed around a queue of algorithms.
        Examples may include: Progress reporting, Quality of Life improvements, multi-threading, etc.

    MTD
        Analysis Data Service (ADS, aka Mantid Data (MTD)), the internal database held by :term:`Mantid` of all *named* workspaces.
        Any workspace referenced "by name", usually by a Mantid algorithm, needs to be registered in this database.

    Native Mode
        The SNAP instrument uses :term:`detectors<Detector>` comprised of many more :term:`pixels<Pixel>` than are actually required to achieve the target d-spacing resolution.
        In *Native* mode, both event data, and physical specifics, from all of these pixels will be used during processing. (See :term:`Lite Mode`)

    Normalization
        The process of adjusting diffraction data to correct for variations in instrumental performance and experimental conditions.
        Normalization ensures that data from different runs or different :term:`instrument states <Instrument State>` can be directly
        compared or combined without bias due to instrument efficiency, sample positioning, or other systemic factors. This is typically
        achieved by dividing the raw data by a normalization standard, such as a vanadium run, which represents the instrument response.
        The process involves a series of algorithms, often encapsulated within a Recipe, to apply these corrections and produce normalized
        data suitable for further analysis or interpretation.

    Orchestration Layer
        The architectural layer that handles the stitching together of the various :term:`Service Components <Service Component>`, :term:`Data Components <Data Component>`, and :term:`Recipe Components <Recipe Component>` to achieve and abstract goal.
        This may include handling :term:`User Requests <User Request>`, or performing :term:`Data State Management`.

    Pixel
        The smallest physical sensing element for particle-flux measurement.
        Detector panels are comprised of pixels.

    Pixel Calibration
        TODO

    Pixel Group
        This is the a pixel grouping scheme that is used within the reduction process.

    Pixel Grouping Parameters
        The expectation value of selected pixel physical-location and d-spacing parameters, taken over a specified grouping schema.
        For each specific parameter, there is one entry for each integer pixel group.

    Pydantic
        A data object serialization and validation framework, implemented in Python.

    Processing Layer
        The architectural layer responsible for implementation level details of the backend.
        This includes things like the :term:`Data Component`, and the :term:`Recipe Component`.

    Reduction
        The process by which raw diffraction data is filtered, distilled into more compact and meaningful data that a scientist may draw conclusions from.

    Recipe
        A collection of algorithms or calculations that are triggered by a request to perform a specific task.
        Examples include: Reduction, Calculate Pixel Grouping Parameters, Purge Overlapping Peaks etc.

        :doc:`Recipe Component <developer/architecture/backend/recipe>`
        The architectural Component that provides an abstraction for the execution of data Calculation and Transformation.
        It is responsible for executing Buisness Logic provided by the Product Owner, and returning the results to the caller.
        Examples include: Reduction, Calculate Pixel Grouping Parameters, Purge Overlapping Peaks etc.

    Recipe Component
        TODO

    Resource
        Small, static configuration data stored within the codebase that may easily be looked up via relative path or key.

    Run
        A single collection of diffraction data that was collected at a specific point in time.
        It is identified by a unique ID, and is associated with a specific Instrument State and Calibration.

    Run Number
        The unique integer identifier of a Run.  Note that certain facilities (e.g. ISIS SANS) may allow the addition of non-integer suffixes to the run number string.

    Service Component
        The architectural Component that provides the individual units of backend fuctionality that a user may interact with.
        Examples include: Data Reduction, Calibration Quality Assessment, Instrument State Initialization, etc.
        It provides this functionality by orchestrating Data and Recipes Components to produce the expected results.

    Smoothing Parameter
        A numerical value used to control the degree of smoothing applied to diffraction data during processing.
        Smoothing is a technique used to reduce noise and enhance signal clarity, making it easier to identify and
        analyze peaks in the data. The smoothing parameter determines the extent of this smoothing effect, with
        higher values leading to a smoother signal. It is often adjusted as part of the :term:`Normalization` or
        :term:`Reduction` processes and is applied via algorithms within a :term:`Recipe`.

    Software Metadata
        This refers data about how SNAPRed operates.
        A prime example of this is the current mappings the InterfaceController has to the various services.
        Another example may be the current version of SNAPRed or its various configurations stored in the :ref:`application.yml <applicationyml>`.

    Spectrum
       A vector of either histogram or event data, consisting of both location (e.g. "x") and counts (or fluence, e.g. "y") values.
       For special applications such as grouping and masking, spectra may be single valued.

    State
        The static positions of the detectors and any other distict configurable characteristics of the :term:`Instrument` recorded as part of a :term:`Run`.

    State Folder
        The folder that contains all configurable data unique to a specific :term:`State`. It is named after the hash of the :term:`State` it represents.

    State ID
       A secure-hash algorithm (SHA) generated ID associated with a specific :term:`instrument state <Instrument State>`.
       This ID is usually represented by its 16-character hexadecimal digest.

    Vanadium
        A reference material commonly used in neutron diffraction experiments for calibration and normalization
        purposes due to its well-understood scattering properties. Vanadium calibration is essential for
        instrument performance verification and for correcting systematic errors in diffraction data. It plays a
        crucial role in the :term:`Calibration` and :term:`Normalization` processes within SNAP, ensuring accurate
        and reliable data analysis.

    User
        TODO

    User Request
        A request made by the backend consumer to perform a specific task given sufficent input data.

    Workspace
        A data object used by mantid to store most data, including neutron scattering data and grouping maps.

    Workspace List
        TODO

    XML
        Extensible Markup Language (XML).
        A fully generalizable text-based markup language, allowing the representation of any type of data.
