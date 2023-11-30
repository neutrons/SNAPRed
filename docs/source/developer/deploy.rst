Deployments
===========

.. _deploy:

Deploying a new release
-----------------------
Execute the following commands substituting the appropriate values for
``<version>``, ``<upstream-branch>``, and ``<branch>``.:

.. code-block:: sh

    git checkout <branch>  # the target release branch
    git rebase -v origin/<branch>  # make sure it is up to date
    git merge --ff-only origin/<upstream-branch> # advance the tip of from upstream-branch
    git tag <version>  # create the release candidate
    git push --tags origin <branch>  # publish the tag


Version Naming Semenatics
--------------------------
Releases
````````
The version number is composed of three parts: ``<major>.<minor>.<patch>``.
The ``<major>`` and ``<minor>`` parts are incremented for major and minor
releases respectively. The ``<patch>`` part is incremented for bugfix
releases. The ``<patch>`` part is reset to ``0`` when the ``<minor>``
part is incremented.

``<major>`` releases signify a breaking change in the API or functionality.
``<minor>`` releases signify a new feature that will not impact existing
functionality. ``<patch>`` releases signify a bugfix that will not impact
existing functionality.

Release Candidates
``````````````````
Release candidates are tagged with the ``rc`` suffix. For example, the
first release candidate for version ``v1.0.0`` is tagged as ``v1.0.0rc1``.

Branch Specific Procedures
--------------------------
The following sections describe the procedures for each branch.

.. _next:

Dev Branch
```````````
The ``dev`` branch is release procedure is very loose and fluid. It is
intended to be a place where new features can be developed and tested
before being merged into the ``qa`` branch for testing. The ``dev``
branch is not intended to be a stable branch.  It is a collaboration
point for devlopers to collect and orient new code such that it is
acceptable for the ``qa`` branch for testing.

The dev version of SNAPRed is released on every merge to the ``dev`` branch.

.. _qa:

QA Branch
`````````
The ``qa`` branch is the branch that is used for testing new features
and bugfixes. It is intended to be a semi-stable branch that allows end users
to test/approve new features and bugfixes before they are merged into the
``main`` branch and released.

The qa version of SNAPRed is released on manual tag and push by a contributor
with appropriate access to the repository.

.. _main:

Main Branch
```````````
The ``main`` branch is the branch that is used for production deployments.
It is intended to be a stable branch representing the latest release of
production code.

The main version of SNAPRed is released manually after a release candidate
has been tested and approved.  The ``main`` branch then follows the same
release procedure as the ``qa`` branch except it is no longer tagged with
the ``rc`` suffix.
