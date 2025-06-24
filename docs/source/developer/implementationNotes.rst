Implementation Notes
====================

Summary:
--------

This page contains special implementation notes.  Most of these
are related to repeated problems that have occurred during
SNAPRed's development phase.

Grouping workspaces:
--------------------

Use of `GroupingWorkspaces` has been problematic in SNAPRed.
Mostly this has to do with the fact that assumptions have been made
about the format of an incoming grouping-workspace, which neither correspond
to how the Mantid code-base actually works, nor to any verified format of
a specific input grouping-workspace.

A `GroupingWorkspace` is a subclass of `SpecialWorkspace2D`, which is a
workspace where each spectrum consists of a single-value.  The single-value in this case
represents the subgroup-ID.  Note that each subgroup-ID may have multiple spectra associated with it.

Accessing the grouping-workspace's spectrum-to-detector map, e.g. using `getDetectorIDs(wi)`, a set of detector-IDs associated with
the spectrum at each workspace-index can be determined.  **However, in almost all cases, to support the most
general format for grouping-workspaces, the method `getDetectorIDsOfGroup(subgroup-ID)` should be used.**
Since the workspace-index itself generally has nothing to do with the subgroup-ID,
it will be emphasized that here that the corresponding subgroup-ID is at `readY(wi)[0]`.
Finally, it isn't even required that a detector-ID participate in only one subgroup, although this assumption
would be a correct for many use cases.

In general, it is recommended to use the `GroupingWorkspace`-specific methods `getGroupIDs()` and `getDetectorIDsOfGroup()`,
and to *avoid* iterating the grouping-workspace's spectra.  When for any reason this is not sufficient,
the spectrum-to-detector map must be used and interpreted correctly,
one needs to be very careful that the most-general format of the `GroupingWorkspace` is supported.

Several algorithms in `Mantid`, such as `DiffractionFocussing`, for convenience encode the subgroup-ID
into the spectrum-number (not the workspace-index) of the spectra in the output workspace. In general,
this behavior should be verified in each case that it is used.  For example, after `DiffractionFocussing`
is applied in `FocusSpectraAlgorithm`: in workspace-index order, the correspondance between the subgroup-IDs
as used by `PixelGroup`, and the spectrum-numbers of the output workspace is verified.
The requirement for this verification is unfortunate, as it would have been straightforward to use a map to
represent these types.  Unfortunately, there still remain several sections in the SNAPRed code-base that do not take this
approach.

To summarize the important points:

  -- grouping-workspaces are essentially a sequence of subgroup-IDs;

  -- the subgroup-IDs may be repeated;

  -- the ordering of subgroup-IDs is arbitrary;

  -- for the spectrum with the value of each subgroup-ID in the sequence,
     its associated detector-IDs belong to the subgroup.
     To assemble the complete set of detector-IDs for the subgroup,
     detector-IDs from all of the participating spectra must be included;

  -- grouping-workspace specific methods should be used wherever possible.


