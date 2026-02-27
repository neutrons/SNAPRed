# Recipes and Algorithms Guide

This guide explains the Recipe system and how to create data processing workflows using Mantid algorithms.

## Overview

**Recipes** compose Mantid algorithms into reusable, type-safe data processing workflows. They're the core of SNAPRed's data processing capabilities.

### Recipe Pattern

A Recipe is a generic class that:
1. Accepts typed **Ingredients** (Pydantic models)
2. Processes them through Mantid algorithms
3. Returns typed **Product** (Pydantic models)
4. Handles errors and edge cases
5. Is reusable across multiple services

### Recipe vs. Algorithm

- **Mantid Algorithm**: Low-level operation on a single workspace
- **Recipe**: High-level workflow combining multiple algorithms + logic
- **Service**: Exposes recipes via HTTP/RPC interface

## Recipe Basics

### Simple Recipe Example

```python
# backend/recipe/MySimpleRecipe.py
from pydantic import BaseModel
from snapred.backend.recipe.Recipe import Recipe
from snapred.backend.recipe.algorithm.Utensils import Utensils

# Define inputs and outputs
class SimpleIngredients(BaseModel):
    workspace_name: str
    smoothing_window: int = 5

class SimpleProduct(BaseModel):
    smoothed_workspace: str
    original_workspace: str

# Create recipe
class MySimpleRecipe(Recipe[SimpleIngredients]):
    """Smooths neutron diffraction data."""
    
    def __init__(self, utensils: Utensils = None):
        super().__init__(utensils)
    
    def cook(self, ingredients: SimpleIngredients) -> SimpleProduct:
        """Execute recipe."""
        # Step 1: Smooth the data
        smooth_alg = self.utensils.getAlgorithm("SmoothData")
        result = smooth_alg(
            InputWorkspace=ingredients.workspace_name,
            OutputWorkspace="smoothed_data",
            WindowLength=ingredients.smoothing_window
        )
        
        # Step 2: Return results
        return SimpleProduct(
            smoothed_workspace=result.OutputWorkspace,
            original_workspace=ingredients.workspace_name
        )

# Usage in service
@Register("/myservice/smooth")
def smooth(self, ingredients: SimpleIngredients) -> SimpleProduct:
    recipe = MySimpleRecipe()
    return recipe.cook(ingredients)
```

## Working with Mantid

### Accessing Algorithms

```python
# Get algorithm by name
alg = self.utensils.getAlgorithm("AlgorithmName")

# Execute algorithm
result = alg(
    InputWorkspace="input",
    OutputWorkspace="output",
    Parameter1=value1,
    Parameter2=value2
)

# Access properties
output_ws = result.OutputWorkspace
output_file = result.OutputFile
```

### Working with Workspaces

```python
# Get workspace from Mantid
workspace = self.mantidSnapper.mtd("workspace_name")

# Get data
x_data = workspace.dataX(0)      # X-axis of spectrum 0
y_data = workspace.dataY(0)      # Y-axis (counts)
e_data = workspace.dataE(0)      # Errors

# Get metadata
num_spectra = workspace.getNumberHistograms()
ws_index = 5
spectrum_num = workspace.getSpectrum(ws_index).getSpectrumNo()

# Iterate spectra
for i in range(workspace.getNumberHistograms()):
    data = workspace.dataY(i)
    # Process spectrum

# Set data
workspace.setData(0, x_vals, y_vals, e_vals)
```

### Saving and Loading

```python
# Save workspace
save_alg = self.utensils.getAlgorithm("SaveNexus")
save_alg(
    InputWorkspace="my_workspace",
    Filename="/path/to/file.nxs"
)

# Load workspace
load_alg = self.utensils.getAlgorithm("Load")
result = load_alg(
    Filename="/path/to/file.nxs",
    OutputWorkspace="loaded_data"
)

# Access loaded workspace
ws = self.mantidSnapper.mtd("loaded_data")
```

## Complex Recipe Example

```python
from typing import List

class CalibrationIngredients(BaseModel):
    raw_workspace: str
    calibration_material: str
    detector_ids: List[int]

class CalibrationProduct(BaseModel):
    offset_workspace: str
    calibration_quality: float
    peak_positions: List[float]

class CalibrationRecipe(Recipe[CalibrationIngredients]):
    """Multi-step calibration workflow."""
    
    def cook(self, ingredients: CalibrationIngredients) -> CalibrationProduct:
        # Step 1: Load calibration standard
        std_data = self._loadCalibrationStandard(
            ingredients.calibration_material
        )
        
        # Step 2: Fit peaks
        peaks = self._fitPeaks(
            ingredients.raw_workspace,
            std_data["theoretical_positions"]
        )
        
        # Step 3: Calculate offsets
        offsets = self._calculateOffsets(
            peaks,
            std_data["theoretical_positions"]
        )
        
        # Step 4: Apply offsets
        offset_ws = self._applyOffsets(
            ingredients.raw_workspace,
            offsets
        )
        
        # Step 5: Assess quality
        quality = self._assessQuality(offset_ws, peaks)
        
        return CalibrationProduct(
            offset_workspace=offset_ws,
            calibration_quality=quality,
            peak_positions=peaks
        )
    
    def _loadCalibrationStandard(self, material: str):
        """Load crystallographic data for calibration material."""
        # Lookup material data
        materials_db = {
            "Si": {
                "theoretical_positions": [1.54, 2.24, 2.66],
                "d_spacing": 3.135
            },
            "CeO2": {
                "theoretical_positions": [1.50, 1.83, 2.10],
                "d_spacing": 2.70
            }
        }
        
        if material not in materials_db:
            raise ValueError(f"Unknown material: {material}")
        
        return materials_db[material]
    
    def _fitPeaks(self, workspace: str, peak_positions: List[float]):
        """Fit peaks to find actual peak centers."""
        fit_alg = self.utensils.getAlgorithm("FindPeaks")
        result = fit_alg(
            InputWorkspace=workspace,
            PeakPositions=peak_positions,
            OutputWorkspace="peak_fit",
            FitWindowWidth=0.1
        )
        
        # Extract fitted peak positions
        peak_table = self.mantidSnapper.mtd("peak_table")
        fitted_positions = []
        for i in range(peak_table.rowCount()):
            fitted_positions.append(peak_table.row(i)["centre"])
        
        return fitted_positions
    
    def _calculateOffsets(
        self,
        fitted_peaks: List[float],
        theoretical_peaks: List[float]
    ):
        """Calculate detector position offsets."""
        offsets = []
        for fitted, theoretical in zip(fitted_peaks, theoretical_peaks):
            offset = theoretical - fitted
            offsets.append(offset)
        
        return offsets
    
    def _applyOffsets(self, workspace: str, offsets):
        """Apply calculated offsets to detectors."""
        adjust_alg = self.utensils.getAlgorithm("AdjustPeaks")
        result = adjust_alg(
            InputWorkspace=workspace,
            Offsets=offsets,
            OutputWorkspace="calibrated"
        )
        return result.OutputWorkspace
    
    def _assessQuality(self, workspace: str, peaks: List[float]):
        """Calculate calibration quality metric."""
        ws = self.mantidSnapper.mtd(workspace)
        
        # Simple metric: standard deviation of peak positions
        mean_peak = sum(peaks) / len(peaks)
        variance = sum((p - mean_peak) ** 2 for p in peaks) / len(peaks)
        quality = 1.0 / (1.0 + variance)  # 0-1 scale
        
        return quality
```

## Generic Recipe Pattern

For common workflows, use `GenericRecipe`:

```python
from snapred.backend.recipe.GenericRecipe import GenericRecipe

class MyGenericRecipe(GenericRecipe[MyIngredients, MyProduct]):
    """Uses GenericRecipe for standard patterns."""
    
    def executeAlgorithms(self, ingredients: MyIngredients):
        """Override to define algorithm chain."""
        # Define algorithms to execute
        algorithms = [
            ("Algorithm1", {
                "InputWorkspace": ingredients.workspace,
                "OutputWorkspace": "step1"
            }),
            ("Algorithm2", {
                "InputWorkspace": "step1",
                "OutputWorkspace": "step2"
            })
        ]
        
        return algorithms
    
    def transformResults(self, algorithm_results):
        """Transform algorithm outputs to product."""
        return MyProduct(
            output_workspace=algorithm_results[-1].OutputWorkspace
        )
```

## Error Handling in Recipes

```python
from snapred.backend.error.SnapredError import SnapredError

class MyRecipe(Recipe[MyIngredients]):
    def cook(self, ingredients: MyIngredients):
        # Validate inputs
        if not ingredients.workspace_name:
            raise SnapredError("Workspace name is required")
        
        try:
            # Access workspace
            ws = self.mantidSnapper.mtd(ingredients.workspace_name)
        except RuntimeError as e:
            raise SnapredError(
                f"Workspace '{ingredients.workspace_name}' not found"
            ) from e
        
        try:
            # Execute algorithm
            alg = self.utensils.getAlgorithm("MyAlgorithm")
            result = alg(
                InputWorkspace=ingredients.workspace_name,
                OutputWorkspace="output"
            )
        except Exception as e:
            raise SnapredError(
                f"Algorithm execution failed: {e}"
            ) from e
        
        return MyProduct(output_workspace=result.OutputWorkspace)
```

## Workspace Naming

SNAPRed provides a workspace naming utility:

```python
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

# Generate consistent workspace names
raw_ws = WorkspaceName.RAW.format(run=12345)
reduced_ws = WorkspaceName.REDUCED.format(run=12345)
calibrated_ws = WorkspaceName.CALIBRATED.format(run=12345)

# Use in recipes
output_ws = reduced_ws
alg = self.utensils.getAlgorithm("SomeAlgorithm")
result = alg(
    OutputWorkspace=output_ws
)
```

## Testing Recipes

```python
import pytest
from snapred.backend.recipe.MyRecipe import MyRecipe, MyIngredients

def test_recipe_with_valid_input():
    """Test recipe with valid data."""
    recipe = MyRecipe()
    ingredients = MyIngredients(
        workspace_name="test_data",
        parameter=1.0
    )
    
    product = recipe.cook(ingredients)
    
    assert product.output_workspace is not None
    assert len(product.results) > 0

def test_recipe_validates_input():
    """Test recipe validation."""
    recipe = MyRecipe()
    
    with pytest.raises(SnapredError):
        recipe.cook(MyIngredients(workspace_name=""))

def test_recipe_error_handling(mock_utensils):
    """Test error handling."""
    recipe = MyRecipe(utensils=mock_utensils)
    
    # Mock algorithm to raise error
    mock_utensils.getAlgorithm.return_value.side_effect = RuntimeError()
    
    with pytest.raises(SnapredError):
        recipe.cook(MyIngredients(workspace_name="data"))
```

## Recipe Organization

**Structure:**
```
backend/recipe/
├── Recipe.py                 # Base class
├── GenericRecipe.py          # Generic pattern
├── algorithm/
│   ├── Utensils.py          # Mantid interface
│   └── common/              # Shared algorithms
├── calibration/
│   ├── CalibrationRecipe.py
│   └── PeakFittingRecipe.py
├── reduction/
│   ├── ReductionRecipe.py
│   └── NormalizationRecipe.py
└── ...
```

**Naming:**
- Recipe files: `<Purpose>Recipe.py`
- Algorithm files: `<Purpose>Algorithm.py`
- Ingredient/Product classes: `<Recipe>Ingredients`, `<Recipe>Product`

## Best Practices

1. **Type Safety**: Always use Pydantic for Ingredients and Products
2. **Validation**: Validate inputs early in `cook()`
3. **Error Messages**: Provide clear, actionable error messages
4. **Logging**: Log significant steps for debugging
5. **Reusability**: Extract common patterns to utility methods
6. **Comments**: Document non-obvious algorithm parameters
7. **Testing**: Test with real Mantid workspaces when possible
8. **Workspace Cleanup**: Clean up intermediate workspaces to save memory

```python
class MyRecipe(Recipe[MyIngredients]):
    def cook(self, ingredients: MyIngredients):
        from snapred.backend.log.logger import snapredLogger
        logger = snapredLogger.getLogger(__name__)
        
        logger.info(f"Starting recipe with {ingredients}")
        
        # ... algorithm execution ...
        
        # Clean up intermediate workspaces
        self.utensils.deleteMantidWorkspace("intermediate_step1")
        self.utensils.deleteMantidWorkspace("intermediate_step2")
        
        logger.info("Recipe completed successfully")
        return product
```

## Common Patterns

### Pattern: Multi-Spectrum Processing
```python
def processAllSpectra(self, workspace: str):
    ws = self.mantidSnapper.mtd(workspace)
    results = []
    
    for i in range(ws.getNumberHistograms()):
        x = ws.dataX(i)
        y = ws.dataY(i)
        e = ws.dataE(i)
        
        processed = self.processSpectrum(x, y, e)
        results.append(processed)
    
    return results
```

### Pattern: Parameter Sweep
```python
def parameterSweep(self, workspace: str, param_values: List[float]):
    results = {}
    
    for param in param_values:
        alg = self.utensils.getAlgorithm("MyAlgorithm")
        result = alg(
            InputWorkspace=workspace,
            Parameter=param,
            OutputWorkspace=f"result_{param}"
        )
        results[param] = result.OutputWorkspace
    
    return results
```

### Pattern: Conditional Branching
```python
def cook(self, ingredients):
    if ingredients.use_fast_method:
        return self._fastProcess(ingredients)
    else:
        return self._accurateProcess(ingredients)

def _fastProcess(self, ingredients):
    # Quick algorithm chain
    pass

def _accurateProcess(self, ingredients):
    # Thorough algorithm chain
    pass
```
