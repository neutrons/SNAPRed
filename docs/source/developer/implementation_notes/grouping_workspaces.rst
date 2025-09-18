Grouping Workspace Fine Points
------------------------------

A `GroupingWorkspace` represents a one-to-many mapping between integer subgroup-IDs, and corresponding sets of pixel-IDs.  As implemented in the Mantid codebase, `GroupingWorkspace` s can be more complex than they initially seem.  Most of the subtlety relates to the fact that there are *multiple* ways to represent any specific subgroup-id to pixel-ids grouping.

A `GroupingWorkspace` is a subclass of `SpecialWorkspace2D`, which is a
workspace where each spectrum consists of a single-value.  The single-value in this case
is the subgroup-ID.  Note that each subgroup-ID may have multiple spectra associated with it.
The subgroup-ID *zero* has a special meaning: which is usually that the associated pixel-IDs should be ignored (e.g. for *masked* pixels).

Accessing the grouping-workspace's spectrum-to-detector map, e.g. using `getDetectorIDs(wi)`, a set of pixel-IDs associated with
the spectrum at each workspace-index can be determined.  Assembling the complete set of pixel-IDs corresponding to a subgroup requires assembling
the spectrum-to-detector map contributions from *all* of the spectra that have that subgroup-ID as a value.
The *possible* number of such spectra ranges from only *one*, to as many spectra as there are pixel-IDs.
That is, there are many alternative *correct* ways to represent the subgroup, and no specific representation
should be depended upon without additional validation.

In general, it is recommended to use the `GroupingWorkspace`-specific methods `getGroupIDs()` and `getDetectorIDsOfGroup()`,
and to *avoid* iterating over the grouping-workspace's spectra.  When for any reason this is not sufficient,
the spectrum-to-detector map must be used and interpreted correctly,
and one needs to be very careful to ensure that the most-general format of the `GroupingWorkspace` is taken into account.

The order of the subgroup-IDs in the grouping workspace cannot be depended upon. It will be emphasized that the subgroup-ID is at `readY(wi)[0]`.  It's important to *validate* any assumption made about the workspace-order at each point-of-use.  Several algorithms in `Mantid`, such as `DiffractionFocussing`, for convenience encode the subgroup-ID
into the spectrum-number (not the workspace-index) of the spectra in the output workspace. In general,
this behavior should be verified in each case that it is used.  For example, after `DiffractionFocussing`
is applied in `FocusSpectraAlgorithm`: the correspondance between the order of the subgroup-IDs
as used by `PixelGroup`, and the workspace-index order of the spectrum-numbers of the output workspace is verified.
As an alternative implementation, it would have been straightforward to use a map to
represent these types.  If this were the case, this ordering ambiguity would no longer be an issue.  However, there still remain several sections in the SNAPRed code-base that do not take this
approach.

Regarding pixel-IDs: it is not required that all of the pixel-IDs have a corresponding subgroup.  In some cases the unused pixels may be placed in subgroup *zero*. At the opposite extreme, it isn't even required that a pixel-ID participate in only *one* subgroup, although this latter assumption
*would* be a correct in many use cases.

To summarize the important points:

  -- Grouping-workspaces are essentially a sequence of subgroup-IDs;

  -- The subgroup-IDs may be repeated: this is why it's important to use `getDetectorIDsOfGroup`;

  -- The ordering of subgroup-IDs is arbitrary: that is, don't depend on workspace-index specifics;

  -- It isn't necessary that the subgroups include *all* pixel-IDs; unused pixels may be grouped into subgroup *zero*, but if this fact were to be used during implementation, it must be verified;

  -- It isn't necessary that a pixel-ID be included in only one subgroup, although usually this will be the case;

  -- Grouping-workspace specific methods should be used wherever possible.  By using these methods,
     most of these representation details will be supported *transparently* by the Mantid codebase itself.
