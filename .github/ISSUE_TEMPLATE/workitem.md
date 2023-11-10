---
name: Work Item
about: Description of EWM work item
title: ''
labels: ''
assignees: ''

---

## Overview
### PR's
Where the pr's related to the story go.
### I/O
Expected Outputs: What the implemented story should produce (high level)
Inputs: Exhaustive list of Mantid/SNAPRed data needed to implement story
	ex. *user touches this button, focussed run data, PixelGroupingParameters, Nothing*

This outlines the scaffolding of what needs to exist before the actual implementation begins.
Working backwards often helps this because it lets you sus out inputs to make things happen.
An Answer may apply to many questions but a Question may only need one good Answer.
### Test Data

A link/path to test data if required.
### Abstract
This section adds context to the story as well as calling out pain-points and unknowns at a high level.  This highlights the point of the story in case it gets lost in a sea of info.

## Details

### Description
Where you go into detail about topics highlighted in the [Abstract](#Abstract) section.  Preferably headed by each topic.

### Acceptance Criteria

This section should contain an exhaustive list of items accomplished by the story.
Please be mindful of non sequiturs, limiting the scope of the story to what one might infer from the title.
If there are UI and Backend components to the Acceptance Criteria, please create separate stories.

### Implementation Suggestions
This Section is mostly for me(Michael Walsh) to outline some concrete steps if possible so that if it requires some orchestration there's less churn finding everything they need in the [I/O](#I/O) section.  This can probably be replaced with good Documentation.
