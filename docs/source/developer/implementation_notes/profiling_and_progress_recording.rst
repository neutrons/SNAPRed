Execution Time Estimator for Computational Processes
====================================================

Background
----------

It is straightforward to mathematically estimate the execution time for a computational process. However, when such an estimate is applied in the real world, it may often end up being *almost useless*. This is typically because:

- The effects of constant terms are neglected at short timescales.
- Processing-system related issues are ignored at long timescales.

For this reason, the current system for execution-time estimation is based on:

- The specification of a computational order, combined with a set of **empirical measurements** of the process behavior.

In combination, these are then used to construct an **estimator function**, which is applied to predict execution-time behavior.

Each measurement consists of:

- A single scalar reference value ``N_ref``, combined with the associated wall-clock execution time for the process step under consideration ``dt``.

An *a priori* computational order (selected from ``O_0``, ``O_N``, ``O_LOGN``, ``O_N2``, ...) is used to convert this ``N_ref`` into a value ``N``, which is then used to construct the estimator function as ``dt(N)``. This construction uses a spline-fitting technique, with a constrained maximum degree.  Such an estimator will produce highly accurate values for ``N_ref`` falling within the interval of the measurements used in its construction, but somewhat less accurate values for ``N_ref`` falling outside of that interval.

----

Design objectives
-----------------

Use an efficient technique to obtain empirical timing measurements from relevant subsections of the running code. This technique should:

- Be as *flexible* and *non-intrusive* as possible

  - Easy for a developer to apply initially
  - Easy to modify where it is applied.

- Provide *automatic* identification of which section of code a measurement comes from.
- Provide *automatic* detection of changes to the ``N_ref`` function used to produce the estimator.  This allows the user to be notified when the existing set of empirical measurements is no longer valid, and should be discarded.

  - Note that detecting changes to the selected *order* is not a requirement, as the estimator can easily be reconstructed from existing measurements when the order is modified.

The technique must work with:

- Methods in a ``Service`` class.
- Methods in a ``Recipe`` class.
- Any subsections of code within those methods.

Both a **decorator form** and **context-manager form** of the ``WallClockTime`` profiler should be provided.

- The decorator must decorate either a class or a function (in the discussion, ``Recipe`` and ``Service.method`` are used as examples).
- When a class is decorated, the method to profile must be specified *explicitly*.  For example ``Recipe.cook`` is usually specified when a ``Recipe`` class is decorated.
- For context-manager application, the local stack frame determines the calling method *automatically*, but this is insufficient to identify the precise location in the code.  This creates the additional requirement to specify an *explicit* step name in this case.
- In all cases, an explicit step name (optional for decorator application) *may* be specified, and in general doing this will provide additional
  clarity to a developer attempting to read and understand the recorded measurement values.

Time estimates should be available whenever required for any *running* process step. The estimated completion time for each step should be **logged**.

- Process steps may be arbitrarily nested.  Logging from process substeps should be *abbreviated*.
- Customized display of in-progress process steps should be facilitated by deriving from the ``Logging.Handler`` class.  For example, in this manner the existing
  logging updates might be used to control a progress bar.  Any required numerical information for such use should be entered into the ``LogRecord.extra`` ``dict`` so that it can be accessed by such a derived ``Handler``.

----

Implementation details
----------------------

The primary class implementing both timing measurement and execution-time estimation is ``ProgressRecorder``.

- Applied by calling ``record()`` at the start of an execution step, and ``stop()`` at the end.
- Exposed via its decorator / context-manager ``WallClockTime``.  This is what should **normally** be used; ``record()`` and ``stop()`` should not be called directly.

Timing data from ``ProgressRecorder`` is **persistent**:

- Stored at: ``${user.application.data.home}/workflows_data/timing``
- Retained timing data is:

  - limited by the number of retained files
  - limited by the size of each file.

Progress-recording steps are automatically named using details from the scope of the decorated function / class, or from the local scope where the context manager is applied. When used as a context manager, an explicit step name must be provided (but this is optional when used as a decorator).

- Explicit and implicit step-name information is *combined* to generate a step key to identify the progress-recording step.
- Steps must be unique under this keying system, but otherwise they may be arbitrarily nested.
- A stack keeps track of which steps are currently being recorded ("active steps").  Steps that become active while others are also active are called "substeps".

----

Logging and estimation
~~~~~~~~~~~~~~~~~~~~~~

For each application, either as a decorator or a context manager, a suitable ``N_ref`` function must be specified.  At the start of each execution step, the current estimator instance is used to predict execution time.  In order to compute this prediction, the current value of ``N_ref`` is required. Often, special cases occur during the calculation of ``N_ref`` which indicate that no estimate can be calculated.  For example, when running the reduction workflow in ``live_data`` mode, no time estimate is presently possible.

- In such cases, the ``N_ref`` function should return ``None``, and the logging for the step will display simply *no data available*.

For a step that is not a substep, an **automatic logging chain** is initiated:

- This chain reports progress of execution at regular intervals, until either the step completes, or the step exceeds its time estimate.
- Logging from substeps is much reduced (no automatic logging chain starts).

Each generated ``LogRecord`` includes details (step key, time estimate, remaining time) in its ``extra`` ``dict`` that will be useful for an ergonomic display.  For example, a class might be derived from ``Logging.Handler``, and then connected to a GUI progress bar.

----

Updating the estimator
~~~~~~~~~~~~~~~~~~~~~~

At successful completion of a step:

- The current measurement is recorded.

  - If an exception occurs, the step is marked as completed (and therefore *inactive*), but no measurement is recorded.

- The current estimate is compared to the actual execution time.

  - If the disparity is above a certain threshold, the estimator updates.

- The estimator function updates only if enough measurements are available (usually this will require at least three).
- Preferably, measurements should be at *distinct* ``N_ref`` values (although this is not strictly required).
- Over time, estimator quality improves rapidly.

----

Known issues and what to expect when testing
--------------------------------------------

- Only primary workflows and a few key recipes have been decorated so far.  It would be possible to decorate all recipes automatically, but this has not been attempted due to the discrepancy between the derivation of ``Recipe`` and ``GenericRecipe``.
- Progress logging is only *activated* for selected target steps.  This means that obtaining as much useful profiling data as possible does not necessarily increase the logging output.

To facilitate testing, a new IPC-based logging feature is provided which allows logging output to be viewed in separate terminal windows:

- IPC-handler names are specified in ``application.yml``.
- Each handler has a list of logger names that are associated with it.

  - Example: the ``ProgressRecorder`` logger associated with the ``SNAPRed-progress`` IPC handler.

- For security reasons, IPC-based logging uses Unix-domain sockets (``UDS``), and not localhost.

At present, the provided logging output for progress recording is not particularly *ergonomic*:

- When displayed in a separate terminal window, the current output might be adequate for some use cases (e.g ``SNAPWrap``).
- A better method to display this data is almost certainly needed for both:

  - ``SNAPRed`` GUI panel
  - ``SNAPRed`` backend via ``SNAPWrap``.

For testing, it may help if you **already have** a diffraction calibration and a normalization calibration for the run numbers you intend to use.  Due to the required ``N_ref`` treatment of *special-cases* mentioned above:

- At present, profiling only covers the case where both a diffraction calibration and a normalization calibration are available.

----

To test - Dev testing
~~~~~~~~~~~~~~~~~~~~~

1. Remove any data files at ``${user.application.data.home}/workflows_data/timing``.

   - These may have been auto-generated.
   - It's OK to simply delete these JSON files, whenever required.

2. Start ``SNAPRed`` (either as ``env=dev python -m snapred``, or from ``mantid_workbench``).
3. Make a note of the process id (PID) using ``ps -u USER | grep python``.  Note here that ``mantid_workbench`` may have launched multiple python processes; use the first one.
4. In a separate terminal (activate the ``SNAPRed`` conda environment), start an IPC-server for the ``SNAPRed-progress`` handler:

   - ``python tests/cis_tests/util/logging/IPC_server.py -n SNAPRed-progress -p ${PID}``

5. Run any workflow, e.g., reduction. Start by reducing data for one run.
6. Exit. In an IPC-logging terminal, type CNTL-C to exit.  Look at the JSON in ``${user.application.data.home}/workflows_data/timing``.  Details of ``dt`` and estimated ``dt_est`` should have been recorded for several service steps and possibly from various recipes, depending on which workflow was executed.  Note that the *default* estimator is linear: 1GB in 3.0s -- this default instance should still be current after running only one workflow.  Check the *automatically* generated step keys.
7. Repeat steps 1-3 to start a new process with an IPC-logger.
8. Run the reduction workflow for several distinct runs (e.g., 58810, 58812, 58813, 59039).
9. Somewhere in the middle of step 8, note that the time estimates indicated in the log will become much more accurate.
10. Exit as before. Inspect the latest JSON files -- estimates should reflect improved accuracy, and you can see that the applied spline degree has increased.

----

Notes on estimator accuracy
~~~~~~~~~~~~~~~~~~~~~~~~~~~

- The default linear estimator is not very accurate.  It will update automatically when enough timing measurements are available.
- The effect of this update can be seen in the JSON data at ``${user.application.data.home}/workflows_data/timing``.
- If *distinct* ``N_ref`` values are not used, the estimator may become accurate for those run numbers only; extrapolation to other run numbers will be less accurate.

  - Regardless, automatic updating continues as needed.

- Given enough distinct ``N_ref`` values, the estimator will quickly become accurate, even when extrapolation is required.

Definition of the ``N_ref`` function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Special cases in workflows complicate the definition of the ``N_ref`` function.  For example, in the normalization workflow, the distinction between cylindrical / spherical samples has not yet been treated.

- The system presently identifies but does not attempt to estimate such special cases.
- When defining an ``N_ref`` method, it's important to return ``None`` if a case is identified that should not be estimated.  This ensures that any recorded measurement can be effectively used to create a *useful* time estimate for the workflow.

  - If every single measurement were recorded, certainly the estimate would be able to predict the *average* execution time for any workflow, but that average might be over cases including so many *special* parameter combinations that it would not be useful to the end user.

The ideal way to fix this issue of special casing the ``N_ref``, in order to allow these estimators to function more broadly, is probably to move such special cases out of the service layer and up to workflow level.  Alternatively, the step *keying* system could be made more elaborate.
