Recipe Component
================

The architectural Component that provides an abstraction for the execution of data Calculation and Transformation.
It is responsible for executing Buisness Logic provided by the Product Owner, and returning the results to the caller.
Examples include: Reduction, Calculate Pixel Grouping Parameters, Purge Overlapping Peaks etc.

Inputs
------

The Recipe Component is a Fucntional Unit that takes a set of inputs, and returns a set Calculated/Transformed outputs.
No reading, no writing, no state.
Simple as.

* Ingredients
    Ingredients are a class of Data Access Objects that are used to provide the Recipe Component with the data(minus workspace references) it needs to perform its calculations.
    They are a self validating DataModel of non-run data that is used to perform the calculations of the Recipe Component.
* Workspaces
    Workspaces are a Mantid concept that house that large sets of data that are used to perform calculations.
    In SNAPRed we typically use these to hold data collected during Runs(Experiment Runs, Vanadium Runs, Calibration Runs etc), as well as DiffCal data.
    More generally, they are used to interect with Mantid.
    Mantid widely integrates workspaces into its API, and as such, they enable the use of a lot of Mantid's functionality. (Algorithms, Plotting, etc)

Outputs
-------

The outputs of the Recipe Component are expected to be a NamedTuple of the calculated/transformed data. (You may find Dicts returned in some places, we would like to move away from this.)
This includes Workspaces that are dirtied or created during the execution of the Recipe Component, as well as any other data that is calculated/transformed.

Recipe Component Execution
--------------------------

The standard practice is through the entry point method `executeRecipe`.
This method takes the Ingredients and Workspaces as inputs, and returns the calculated/transformed data as outputs.

Generic Recipes
'''''''''''''''

It is expected that a lot of recipes will just be a wrapper around a Mantid Algorithm.
In these cases, we have a Generic Recipe class that can be used to simplify the creation of these recipes.
In the Generic Recipe file you will declare a new class that inherits from the Generic Recipe class with the Template Argument being the Mantid Algorithm you are wrapping.
This will simply forward the inputs to the Mantid Algorithm, and return the outputs.


Standard Practices
''''''''''''''''''

After implementing a few recipes, you will notice that there are a few orders of operations that may occur.
These are:

* Chop Ingredients
    Data provided to Recipes is in the form of Conceptual Data Access Objects, and as such it is often useful to extract necessary data from these nested objects.
    The Chop Ingredients step flattens the data to a set of local variables by 'chopping' the ingredients.
    I like the visual metaphor of segmentation along the dot operators.
* Portion Ingredients
    The Portion Ingredients step is a preprocessing step that is used to create new Ingredients from the chopped ingredients.
    This is relevant if your recipe is a meta recipe that is used to execute multiple sub recipes.
    This helps streamline recipes often executed together.
* Unbag Groceries
    Similar to the Chop Ingredients step, the Unbag Groceries is a preprocessing step, but it is aimed at Mantid Workspaces.
    This is where you may want to initially convert units or create clones.
* Execute Recipe
    Finally, onces all the input data is properly peeled, carved, and chopped, the Recipe Component is ready to execute.
    This is where the Buisness Logic is executed, and the calculated/transformed data is returned.
    This section should be as much of a straight shot as possible, with minimal branching.
