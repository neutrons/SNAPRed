<!-- 
	task.md
	Version 2.1
	This is a GitLab Issue description template to be used as a unit of work for implementing a user story
	
	Note: The Task title should be either: 
		- [Task] Imp Story <technique acronym> #<user story number>: <clear descriptive title>
			- Technique acronyms: SCD, PD, IM, REF, SANS, ED, DGSTAS, IGS, SE

		- [Task] Defect <technique acronym> #<defect number>: <clear descriptive title>

		- PLEASE USE THE TECHNIQUE ACRONYM TO SAVE SPACE

	Add links to GitLab merge requests (MR)
		- Create the MR from the issue
		- Reference the issue in a MR

	Add links to GitHub pull request (PR)
		- [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/<github project/repo/pr#>)(<link to github pr>)
		- EXAMPLE: PR's against `master`: [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/mantidproject/mantid/31712)](https://github.com/mantidproject/mantid/issues/31712)
-->

Description:
===========


List of Tasks:
---------------
* [ ]  Understand Story: Talk to owner
* [ ]  Address Necessary Revisions to the Story
       <!-- Add a comment below to capture revisions-->
* [ ]  Capture Significant Design Decisions
* [ ]  Gather All Required Test Data
       <!-- Add a comment below to capture revisions-->
       <!-- Where is it, where did you get it, etc.? -->
* [ ]  Write & Run Unit Tests
* [ ]  Implement Story (Update unit test as needed)
* [ ]  Write & Run Integration Tests
* [ ]  Submit Pull Request
* [ ]  Report Completion During Status Meeting

<!-- Note: Adjust task details as necessary
     Note: Use the GitLab Related relationship for related Tasks
           Use the GitLab Blocked By / Blocks relationship for blocked Tasks
		   Use the GitLab Blocked By / Blocks for relationships with Stories and Defects
	 Note: The default milestone will override a manually set milestone.  
	       Remove default milestone if setting manually
-->

/label ~"IssueType::Task"
/label ~"State::5-Accepted"
/milestone %Backlog
