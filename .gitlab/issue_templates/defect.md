<!-- 
	defect.md
	Version 2.2
	This is a GitLab Issue description template to be used to create a defect
	
	Provide a unique descriptive title with "[Defect]"
		- [Defect] <unique descriptive title>

	Add links to GitLab merge requests (MR)
		- Create the MR from the issue
		- Reference the issue in a MR

	Add links to GitHub pull request (PR)
		- [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/<github project/repo/pr#>)(<link to github pr>)
		- EXAMPLE: PR's against `master`: [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/mantidproject/mantid/31712)](https://github.com/mantidproject/mantid/issues/31712)
-->

Problem Description:
====================


Steps to Reproduce:
-------------------


Investigation/Analysis Results:
-------------------------------


<!-- Note: Use the GitLab Related relationship for related Defects
           Use the GitLabe Blocked By / Blocks relationship for Tasks that resolve the Defect
           Use the GitLabe Blocked By / Blocks relationship for blocking stories
-->

/label ~"IssueType::Defect"
/label ~"State::1-Draft"
/milestone %Backlog
