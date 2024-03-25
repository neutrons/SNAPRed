DiffractionCalibrationRecipe Class Documentation
================================================

DiffractionCalibrationRecipe orchestrates a detailed diffraction calibration process
for scientific data, leveraging Pydantic models to structure and process calibration
inputs. This class meticulously handles the preparation of DiffractionCalibrationIngredients,
setting up necessary workspaces, and executing calibration through pixel and group-based
algorithms.


Process Overview:
-----------------

- Ingredient Preparation: Utilizes chopIngredients to extract essential calibration settings
  from DiffractionCalibrationIngredients, preparing the groundwork for calibration.

- Workspace Setup: Employs unbagGroceries to organize required workspaces, delineating input,
  output, and intermediary workspaces crucial for calibration tasks.

- Calibration Execution: Central to the class, executeRecipe melds preparation steps to perform
  calibration, employing both pixel and group-based algorithms. It iteratively adjusts offsets
  and refines calibration parameters, aiming for convergence within set thresholds or maximum
  iterations.


Key Functionalities:
--------------------

- chopIngredients: Parses and prepares calibration settings, including run number, convergence
  threshold, and maximum iterations, from provided ingredients.

- unbagGroceries: Maps out necessary workspace names for calibration, detailing input, grouping,
  output, calibration table, and mask workspaces.

- executeRecipe: Executes the calibration process, leveraging PixelDiffractionCalibration and
  GroupDiffractionCalibration algorithms. It manages iterative adjustments to achieve convergence,
  meticulously documenting each step's outcomes and ensuring calibration accuracy.


Operational Details:
--------------------

1. Initializes workspace names and calibration settings.
2. Begins with pixel-based calibration adjustments, aiming for offset convergence.
3. Proceeds with group-based fitting calibration, refining calibration for each pixel group.
4. Monitors and documents each calibration step, ensuring methodical progress towards calibration accuracy.
