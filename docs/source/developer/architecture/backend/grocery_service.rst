Grocery Service
================

A data service specializing in loading workspace data from disk into mantid's ADS, for use inside algorithms.
The grocery service handles loading neutron scattering data, grouping workspaces, calibration tables, and masking workspaces.
The goal of the grocery service is to centralize and minimize loading operations.
On the first load of a file, it is cached in its initial condition.
Any future operations calling for the same workspace data will clone a copy of the cached workspace,
making it safe to deform the clone.

The primary way to interact with the grocery service is to send it a grocery list.
A grocery list is a list of `GroceryListItem`s.  Each item contains the data needed for the grocery service
to locate the correct file and load the file into a workspace in the ADS.
Which properties of a `GroceryListItem` must be set will depend on the kind of data it corresponds to.
The service will properly handle loading all the items on the list into the ADS then return to you a list of loaded workspaces.

The `GroceryListBuilder` is intended to handle constructing grocery lists or dictionaries.
These can be be passed to either the `fetchGroceryList` or the `fetchGroceryDict` methods inside grocery service.

`GroceryListBuilder`
---------------------

Inside most services, this will usually be initialized inside the `__init__`, called `groceryClerk`.

Any service methods which need to get workspace data can then create their list using

.. code-block:: python
    self.groceryClerk.neutron(<runNumber>).useLiteMode(<useLiteMode>).add()
    self.groceryClerk.grouping(<focusGroupName>).fromRun(<runNumber>).useLiteMode(<useLiteMode>).add()
    groceryList = self.groceryClerk.buildList()

A preferable alternative, for easier use inside algorithms and recipes, is to create a grocery dictionary,
which will assign each loaded workspace a name corresponding to an algorithm's inputs.

.. code-block:: python
    self.groceryClerk.name("InputWorkspace").neutron(<runNumber>).useLiteMode(<useLiteMode>).add()
    self.groceryClerk.name("GroupingWorkspace").grouping(<focusGroupName>).fromRun(<runNumber>).useLiteMode(<useLiteMode>).add()
    groceryDict = self.groceryClerk.buildDict()

If only a single workspace is needed, then a single `GroceryListItem` can be created with

.. code-block:: python
    groceryItem = self.groceryClerk.neutron(<runNumber>).useLiteMode(<useLiteMode>).build()


`fetchGroceryList`
------------------

Make a list of `GroceryListItem`s specofying all needed workspaces for an operation, then pass the list.
This method will handle finding the correct loders.

Inputs
''''''

* groceryList: List[GroceryListItem]
    A list of `GroceryListItems`s, preferably created as above using the builder.

Outputs
'''''''

Returns a list of `WorkspaceName`s corresponding to the requested data.

`fetchGroceryDict`
------------------

Make a dictionary that matches a `GroceryListItem` to a string name.
The string name is NOT the workspace name, but should correspond to the property name inside a recipe or algorithm.
For instance, "InputWorkspace", "BackgroundWorkspace", or "CalibrationWorkspace".

Inputs
''''''

* groceryDict: Dict[str, GroceryListItem]
    A dictionary that matches a property name to a `GroceryListItem`.

Outputs
'''''''

Returns a dictionary that matches the original keys from the input, to the loaded workspace names.


.. `fetchWorkspace`
.. ----------------

.. Returns a dictionary with the following keys:

.. * result: boolean
..     A boolean value, `True` if the file was loaded successfully.
.. * loader: str
..     The loader that was used by mantid, for future reference.
.. * workspace: WorkspaceName
..     The name of the workspace that was loaded into the ADS (same as the passed name argument).

.. `fetchNeutronDataSingleUse`
.. ------------------

.. Will load neutron scattering data without any caching.
.. This can save time and memory, but only use if you are very sure the same data will not be needed later.

.. Inputs
.. ''''''
.. * runId: str
..     The run number given to the
