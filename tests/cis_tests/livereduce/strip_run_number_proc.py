# A livereduce processing script, for testing: it deletes the 'run_number' log from each chunk, as for live
#   data between runs, or before a run has been set up.  The post-processing script then registers the
#   accumulation workspace under `ReservedRunNumber.LIVE`.
#
# `run_startlivedata.py --no-run-number` uses it.  To use it under livereduce, copy it as
#   'reduce_ADARA_FileReader_live_proc.py' into the script directory: livereduce finds its scripts by name.
#
# `StartLiveData` runs this with `input`: the chunk, and `output`: the name of its output workspace.

from mantid.simpleapi import CloneWorkspace, DeleteLog, mtd

CloneWorkspace(InputWorkspace=input, OutputWorkspace=output)  # noqa: F821  (defined by `StartLiveData`)
if mtd[output].getRun().hasProperty("run_number"):  # noqa: F821
    DeleteLog(Workspace=output, Name="run_number")  # noqa: F821
