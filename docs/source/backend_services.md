# Backend Services Guide

This guide explains the core backend services and how to use them accurately.

## Overview

Backend services handle business logic, orchestrate data processing, and interface with Mantid. All services follow the Service pattern: they're singletons registered with paths that can be called via requests.

## Service Routing

All service operations are called through the `InterfaceController` using `SNAPRequest`:

```python
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.api.InterfaceController import InterfaceController

controller = InterfaceController()
request = SNAPRequest(
    path="/service_name/operation_name",
    payload=input_data
)
result = controller.executeRequest(request)
```

The path format is: `/{service_name}/{operation_name}`

## Core Services

### ReductionService
Orchestrates reduction workflows for neutron diffraction data.

**Path prefix:** `reduction`

**Key operations:**
- `""` (empty) - Main reduction operation
- `"validate"` - Validate reduction request before processing
- `"groupings"` - Fetch reduction groupings for a run
- `"ingredients"` - Prepare reduction ingredients
- `"groceries"` - Fetch required workspaces and data
- `"load"` / `"save"` - Load/save reduction state
- `"artificialNormalization"` - Create synthetic normalization

**Usage example:**
```python
from snapred.backend.dao.request import ReductionRequest
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.api.InterfaceController import InterfaceController

# First validate
validate_request = ReductionRequest(
    runNumber="12345",
    useLiteMode=False,
    versions=(1, 1)  # (calibration_version, normalization_version)
)
request = SNAPRequest(
    path="/reduction/validate",
    payload=validate_request
)
controller = InterfaceController()
result = controller.executeRequest(request)  # Returns None if valid

# Then execute reduction
request = SNAPRequest(
    path="/reduction/",  # Note: empty path for main operation
    payload=validate_request
)
result = controller.executeRequest(request)  # Returns ReductionResponse
```

### CalibrationService
Manages instrument calibration workflows.

**Path prefix:** `calibration`

**Key operations:**
- `"diffraction"` - Diffraction calibration (pixel-level)
- `"pixel"` - Pixel-by-pixel calibration
- `"group"` - Group-level calibration
- `"assessment"` - Assess calibration quality
- `"focus"` - Focus spectra
- `"fitpeaks"` - Fit peaks for calibration
- `"residual"` - Calculate calibration residual
- `"fetchMatches"` - Find matching calibrations
- `"lock"` - Lock a calibration
- `"index"` - Get calibration index

**Usage:**
```python
from snapred.backend.dao.request import DiffractionCalibrationRequest
from snapred.backend.dao.state.FocusGroup import FocusGroup

# Setup diffraction calibration
focus_group = FocusGroup(name="Group1", ...)  # Full FocusGroup object
calibration_request = DiffractionCalibrationRequest(
    runNumber="12345",
    calibrantSamplePath="/path/to/sample.cif",
    focusGroup=focus_group,
    useLiteMode=False
)

request = SNAPRequest(
    path="/calibration/diffraction",
    payload=calibration_request
)
result = controller.executeRequest(request)
```

### NormalizationService
Handles normalization workflows (vanadium, monitor, detector response).

**Path prefix:** `normalization`

**Key operations:**
- `""` (empty) - Main normalization operation
- `"smooth"` - Smooth data excluding peaks
- `"assessment"` - Assess normalization quality
- `"fetchMatches"` - Find matching normalizations
- `"calculateResidual"` - Calculate residual after smoothing
- `"lock"` - Lock a normalization
- `"validateWritePermissions"` - Check write access

**Usage:**
```python
from snapred.backend.dao.request import NormalizationRequest

normalization_request = NormalizationRequest(
    runNumber="12345",
    useLiteMode=False
)

request = SNAPRequest(
    path="/normalization/",
    payload=normalization_request
)
result = controller.executeRequest(request)  # Returns NormalizationResponse
```

### LiteDataService
Creates compressed lite-mode data for efficient processing.

**Service name:** `createLiteData` (single operation)

**Method:**
- `createLiteData` - Create lite data from full resolution data

**Note:** Uses `registerPath` instead of `@Register` decorator. Called with `model_dump_json()`.

**Usage:**
```python
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.backend.api.InterfaceController import InterfaceController

# Direct method call (LiteDataService is special)
lite_service = LiteDataService()
data, tolerance = lite_service.createLiteData(
    inputWorkspace=WorkspaceName("raw_data"),
    outputWorkspace=WorkspaceName("lite_data"),
    instrumentDefinition=None,
    export=True
)
```

### ConfigLookupService
Retrieves configuration and grouping data for runs.

**Path prefix:** `config`

**Key operations:**
- `""` (empty) - Get reduction state configs for runs
- `"samplePaths"` - Get available sample file paths
- `"groupingMap"` - Get grouping map for a run

**Usage:**
```python
from snapred.backend.dao.RunConfig import RunConfig

# Get configs for multiple runs
run_configs = [
    RunConfig(runNumber="12345", useLiteMode=False),
    RunConfig(runNumber="12346", useLiteMode=False)
]

request = SNAPRequest(
    path="/config/",
    payload=run_configs  # Will be serialized
)
result = controller.executeRequest(request)
# Returns dict: {runNumber: reductionState, ...}
```

### CrystallographicInfoService
Ingests and processes crystallographic data (CIF files) for calibration.

**Path prefix:** `ingestion`

**Operations:**
- `""` (empty) - Ingest CIF file and extract peaks

**Usage:**
```python
request = SNAPRequest(
    path="/ingestion/",
    payload=None
)
# Called with keyword args via decorator
result = controller.executeRequest(
    SNAPRequest(path="/ingestion/", 
    payload=None)
)
```

### CalibrantSampleService
Manages calibrant sample data.

### WorkspaceService
Manages Mantid workspaces (list, clear, rename, etc.).

**Operations:**
- `"getResidentWorkspaces"` - List current workspaces
- `"renameFromTemplate"` - Rename workspaces using template
- `"clearWorkspaces"` - Delete workspaces

## Request and Response Objects

All service inputs/outputs use typed DAO objects from `snapred.backend.dao.request` and `snapred.backend.dao.response`.

### Actual Request Types (Examples)
```python
# Reduction
from snapred.backend.dao.request import ReductionRequest
request = ReductionRequest(
    runNumber="12345",
    useLiteMode=False,
    versions=(1, 1),
    focusGroups=[]
)

# Calibration
from snapred.backend.dao.request import DiffractionCalibrationRequest
request = DiffractionCalibrationRequest(
    runNumber="12345",
    calibrantSamplePath="/path/to/sample.cif",
    focusGroup=focus_group,
    useLiteMode=False
)

# Normalization
from snapred.backend.dao.request import NormalizationRequest
request = NormalizationRequest(
    runNumber="12345",
    useLiteMode=False
)

# Config lookup
from snapred.backend.dao.RunConfig import RunConfig
configs = [
    RunConfig(runNumber="12345", useLiteMode=False),
    RunConfig(runNumber="12346", useLiteMode=False)
]
```

### Response Types
- `ReductionResponse` - Contains reduced workspaces
- `NormalizationResponse` - Contains normalized data
- `CalibrationAssessmentResponse` - Calibration quality metrics
- `SNAPResponse` - Generic response with status code and data

## Service Architecture

### ServiceDirectory and Registration

Services register their paths automatically via decorators or manual registration:

```python
# Using @Register decorator (modern)
@Singleton
class MyService(Service):
    @Register("myoperation")
    def myOperation(self, request: MyRequest):
        return result

# Using registerPath (older style)
@Singleton
class OldService(Service):
    def __init__(self):
        super().__init__()
        self.registerPath("", self.mainOperation)
        self.registerPath("secondary", self.secondaryOp)
```

Paths are resolved at runtime based on service name and operation path.

## Working with Recipes

Services delegate to Recipes for complex data processing.

### Real Recipe Example from ReductionService
```python
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe

@Register("")
def reduction(self, request: ReductionRequest):
    # Prepare ingredients and groceries
    ingredients = self.prepReductionIngredients(request)
    groceries = self.fetchReductionGroceries(request)
    
    # Execute recipe
    recipe = ReductionRecipe()
    result = recipe.cook(ingredients, groceries)
    
    return ReductionResponse(...)
```

### Recipe Pattern with Type Safety
```python
from snapred.backend.recipe.Recipe import Recipe
from pydantic import BaseModel

class MyIngredients(BaseModel):
    workspace_name: str
    parameter: float

class MyProduct(BaseModel):
    result_workspace: str
    quality: float

class MyRecipe(Recipe[MyIngredients]):
    def cook(self, ingredients: MyIngredients) -> MyProduct:
        # Use self.utensils and self.mantidSnapper
        alg = self.utensils.getAlgorithm("AlgorithmName")
        result = alg(
            InputWorkspace=ingredients.workspace_name,
            OutputWorkspace="output"
        )
        return MyProduct(
            result_workspace="output",
            quality=1.0
        )
```

## Data Structures (DAOs)

Request/Response objects are Pydantic models in `snapred.backend.dao.request` and `snapred.backend.dao.response`.

### Common Patterns
```python
from pydantic import BaseModel, Field
from typing import Optional, List

class MyRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
    optionalField: Optional[str] = None
    listField: List[int] = []
    
    # Validators
    from pydantic import field_validator
    
    @field_validator("runNumber")
    @classmethod
    def validate_run(cls, v):
        if not v or len(v) == 0:
            raise ValueError("runNumber required")
        return v
```

## Error Handling

Services validate inputs and raise appropriate errors:

```python
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.RecoverableException import RecoverableException

@Register("validate")
def validateReduction(self, request: ReductionRequest):
    if not self.dataFactoryService.stateExists(request.runNumber):
        raise RecoverableException.stateUninitialized(
            request.runNumber, 
            request.useLiteMode
        )
    
    continueFlags = ContinueWarning.Type.UNSET
    if not self.dataFactoryService.normalizationExists(request.runNumber):
        continueFlags |= ContinueWarning.Type.MISSING_NORMALIZATION
    
    return continueFlags
```

### Exception Types
- `ContinueWarning` - Recoverable issues (user can proceed)
- `RecoverableException` - Known error states
- `StateValidationException` - Invalid instrument state
- Generic exceptions are caught and converted to HTTP responses

## Configuration

Services access configuration via `Config`:

```python
from snapred.meta.Config import Config

# In a service or recipe
timeout = Config["mantid.timeout"]
instrument_name = Config["instrument.name"]
peakFunction = SymmetricPeakEnum[Config["calibration.diffraction.peakFunction"]]
```

Configuration uses dot-notation paths and supports defaults.

## Logging

Automatic logging via instance logger:

```python
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)

@Register("myoperation")
def myOperation(self, request: MyRequest):
    logger.info(f"Starting operation for run {request.runNumber}")
    try:
        result = self.processRequest(request)
        logger.debug(f"Operation completed successfully")
        return result
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        raise
```

## Real-World Usage Examples

### From UI Presenter: Loading Assessment Data
```python
# From CalibrationAssessmentPresenter
from snapred.backend.dao.request import CalibrationLoadAssessmentRequest

payload = CalibrationLoadAssessmentRequest(
    runId=runId,
    useLiteMode=useLiteMode,
    version=version,
    checkExistent=True,
)
request = SNAPRequest(path="/calibration/loadQualityAssessment", payload=payload.json())
response = controller.executeRequest(request)
```

### From UI Workflow: Workspace Renaming
```python
# From WorkflowImplementer
from snapred.backend.dao.request import RenameWorkspacesFromTemplateRequest

payload = RenameWorkspacesFromTemplateRequest(
    workspaces=["workspace_1", "workspace_2"],
    renameTemplate="{workspaceName}_{iteration:02d}",
)
response = self.request(path="workspace/renameFromTemplate", payload=payload.model_dump_json())
```

### From Workflow: Config Lookup
```python
# From a workflow requesting configuration
from snapred.backend.dao.RunConfig import RunConfig

configs = [
    RunConfig(runNumber="12345", useLiteMode=False),
]
request = SNAPRequest(path="/config/", payload=configs)
result = controller.executeRequest(request)
# Returns: {runNumber: reductionState, ...}
```

## Decorators Used in Services

### @Singleton
Service is instantiated once and reused:
```python
@Singleton
class MyService(Service):
    pass
```

### @Register(path)
Exposes a method as a service endpoint:
```python
@Register("myoperation")
def myOperation(self, request):
    pass
```

### @FromString
Deserializes string payload automatically (JSON → object):
```python
@FromString
def myOperation(self, requests: List[RunConfig]):
    pass
```

### @ConfigDefault
Provides default values from Config:
```python
@ConfigDefault
def ingest(
    self,
    cifPath: str,
    crystalDMin: float = ConfigValue("constants.CrystallographicInfo.crystalDMin"),
):
    pass
```

## Best Practices

1. **Type Safety**: Always use Pydantic request/response objects
2. **Validation**: Validate inputs early in service method
3. **Error Handling**: Provide specific exception types with messages
4. **Logging**: Log at key decision points and errors
5. **Recipes**: Delegate complex processing to Recipe objects
6. **Testing**: Test with actual request payloads
7. **Documentation**: Note path format and payload structure
8. **Decorators**: Use proper decorators for automatic features

## Example: Real Service Call Flow

```python
# 1. UI presenter creates request with DAO
from snapred.backend.dao.request import NormalizationRequest

payload = NormalizationRequest(
    runNumber="12345",
    useLiteMode=False
)

# 2. Create SNAPRequest with service path and payload
request = SNAPRequest(
    path="/normalization/",  # Empty path = default operation
    payload=payload
)

# 3. Send through InterfaceController
controller = InterfaceController()
response = controller.executeRequest(request)

# 4. Response contains SNAPResponse with code and data
from snapred.backend.dao.SNAPResponse import ResponseCode

if response.code == ResponseCode.OK:
    normalization_data = response.data
else:
    error_msg = response.message
```

## Debugging Services

Enable debug logging:
```python
from snapred.meta.Config import Config
Config["logging.level"] = "DEBUG"
```

Add breakpoints in service methods:
```python
@Register("myoperation")
def myOperation(self, request):
    import pdb; pdb.set_trace()
    # Debug here
```

Inspect workspaces in recipes:
```python
# In a recipe cook() method
ws = self.mantidSnapper.mtd("workspace_name")
print(f"Workspace info:")
print(f"  Spectra: {ws.getNumberHistograms()}")
print(f"  X data: {ws.dataX(0)}")
```

## Performance Considerations

- Services process synchronously; avoid blocking on long Mantid operations
- Use WorkerPool from UI layer for async execution
- Cache expensive computations via GroceryService
- Profile with `snapred.backend.profiling.ProgressRecorder`
