# livereduce post-processing script, for a NeXus file replayed by Mantid's `FileEventDataListener`.
#
# livereduce names its scripts after the instrument's short name: the replay instrument is
# 'ADARA_FileReader' (facility 'TEST_LIVE').  For the real SNAP listener, the same body goes in
# `reduce_SNAP_live_post_proc.py`.
#
# `StartLiveData` runs this with `input`: the accumulation workspace, and `output`: the name of its
# output workspace.  `run_livereduce.py` puts this directory on `sys.path`.

from mantid.simpleapi import CreateSingleValuedWorkspace
from snapred_live_reduction import reduceAccumulation

reduceAccumulation(input)  # noqa: F821  (defined by `StartLiveData`)

# `StartLiveData` requires the output workspace, but nothing uses it: a copy of the accumulation workspace
#   would double its memory.
CreateSingleValuedWorkspace(OutputWorkspace=output)  # noqa: F821
