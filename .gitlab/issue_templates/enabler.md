<!-- 
	enabler.md
	Version 1
	This is a GitLab Issue description template to be used as the anchor for a large activity that
	is not directly related to any User Story or Defect.
	It is expected that an Enabler will have child Tasks to cover specific pieces of work.
	Note: An Enabler uses include, but not limited to:
		- Technical Debt
		- Architecture/Design work in anticipation of upcoming capabilities
		- Environment setup
		- etc.
	
	Note: The Enabler title should be either: 
		- [Enabler] <unique descriptive title>

	Add links to GitLab merge requests (MR)
		- Create the MR from the issue
		- Reference the issue in a MR

	Add links to GitHub pull request (PR)
		- [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/<github project/repo/pr#>)(<link to github pr>)
		- EXAMPLE: PR's against `master`: [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/mantidproject/mantid/31712)](https://github.com/mantidproject/mantid/issues/31712)

-->

Description:
===========


Outline of activity:
---------------
* [ ]  \<notional task 1\>

<!-- 
	Note: Child tasks should be created when at the start of the
	iteration the Enabler is scheduled for
    Note: Use the GitLab Blocked By / Blocks relationship for
	      child Tasks
	Note: The default milestone will override a manually set milestone. 
		Remove default milestone if setting manually
-->

/label ~"IssueType::Enabler"
/label ~"State::1-Draft"
/milestone %Backlog
