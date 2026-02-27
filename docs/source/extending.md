# Extending SNAPRed

This guide explains how to extend SNAPRed with new functionality. SNAPRed is designed around clear extension points for services, recipes, and UI workflows.

## Quick Start

### Structure Overview
```
src/snapred/
├── backend/
│   ├── service/       ← Add new Service subclasses
│   ├── recipe/        ← Add new Recipe subclasses
│   ├── dao/          ← Define data structures
│   └── api/          ← Routing and request handling
└── ui/
    ├── presenter/    ← Connect UI to services
    ├── view/        ← PyQt5 widgets
    └── model/       ← UI data models
```

## Creating a New Service

Services encapsulate business logic and are automatically registered via decorators.

### Step 1: Define Data Structures (DAO)
Create a file in `backend/dao/` for your data contracts:

```python
# backend/dao/MyOperationRequest.py
from pydantic import BaseModel

class MyInput(BaseModel):
    run_number: int
    parameter: str

class MyOutput(BaseModel):
    result: float
    status: str
```

### Step 2: Create the Service
Create a file in `backend/service/`:

```python
# backend/service/MyService.py
from snapred.backend.service.Service import Service, Register
from snapred.meta.decorators.Singleton import Singleton
from snapred.backend.dao.MyOperationRequest import MyInput, MyOutput

@Singleton
class MyService(Service):
    def name(self):
        return "MyService"

    @Register("/myservice/operation")
    def performOperation(self, input_data: MyInput) -> MyOutput:
        """
        Process the input and return results.
        """
        result = process_data(input_data.run_number, input_data.parameter)
        return MyOutput(
            result=result,
            status="success"
        )
```

**Key points:**
- Use `@Singleton` so only one instance exists
- Use `@Register(path)` to expose methods via routing
- Accept typed DAO objects as parameters
- Return typed DAO objects

### Step 3: Register Service
Add to `backend/service/__init__.py`:

```python
from .MyService import MyService

__all__ = ["MyService"]
```

### Step 4: Call from UI
In a presenter or handler:

```python
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.api.InterfaceController import InterfaceController

request = SNAPRequest(
    path="/myservice/operation",
    payload=MyInput(run_number=12345, parameter="value")
)

controller = InterfaceController()
result = controller.executeRequest(request)  # Returns MyOutput
```

## Creating a Recipe

Recipes are data processing workflows that compose Mantid algorithms.

### Step 1: Define Ingredients and Products
```python
# backend/recipe/MyRecipe.py
from pydantic import BaseModel
from snapred.backend.recipe.Recipe import Recipe

class MyIngredients(BaseModel):
    workspace_name: str
    smoothing_factor: float = 1.0

class MyProduct(BaseModel):
    output_workspace: str
    peak_positions: list[float]
```

### Step 2: Implement the Recipe

```python
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.Recipe import Recipe

class MyRecipe(Recipe[MyIngredients]):
    """
    Processes a workspace through multiple steps.
    """

    def __init__(self, utensils: Utensils = None):
        super().__init__(utensils)

    def cook(self, ingredients: MyIngredients) -> MyProduct:
        """
        Execute the recipe on the provided ingredients.
        """
        # Access Mantid algorithms via self.utensils
        mantid_alg = self.utensils.getAlgorithm("SomeAlgorithm")

        # Execute algorithm
        result = mantid_alg(
            InputWorkspace=ingredients.workspace_name,
            OutputWorkspace="temp_output",
            Factor=ingredients.smoothing_factor
        )

        # Extract data
        workspace = self.mantidSnapper.mtd(result.OutputWorkspace)
        peak_positions = extract_peaks(workspace)

        return MyProduct(
            output_workspace=result.OutputWorkspace,
            peak_positions=peak_positions
        )

def extract_peaks(workspace):
    # Implementation
    pass
```

### Step 3: Use Recipe in Service

```python
@Singleton
class MyService(Service):
    @Register("/myservice/process")
    def process(self, ingredients: MyIngredients) -> MyProduct:
        recipe = MyRecipe()
        return recipe.cook(ingredients)
```

**Recipe guidelines:**
- Accept Pydantic `BaseModel` ingredients
- Return Pydantic `BaseModel` products
- Use `self.utensils` for Mantid access
- Use `self.mantidSnapper` to access workspaces
- Include error handling for invalid inputs

## Adding UI Workflows

Workflows are multi-step user interactions.

### Step 1: Create a View
```python
# ui/view/MyView.py
from qtpy import QtWidgets, QtCore

class MyView(QtWidgets.QWidget):
    # Define signals for user actions
    actionTriggered = QtCore.Signal(object)  # Emit data

    def __init__(self):
        super().__init__()
        self.setupUI()

    def setupUI(self):
        layout = QtWidgets.QVBoxLayout()
        self.input_field = QtWidgets.QLineEdit()
        button = QtWidgets.QPushButton("Process")
        button.clicked.connect(self.onButtonClicked)
        layout.addWidget(self.input_field)
        layout.addWidget(button)
        self.setLayout(layout)

    def onButtonClicked(self):
        data = {"value": self.input_field.text()}
        self.actionTriggered.emit(data)

    def showResult(self, result):
        QtWidgets.QMessageBox.information(self, "Result", f"Result: {result}")
```

### Step 2: Create a Presenter
```python
# ui/presenter/MyPresenter.py
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.api.InterfaceController import InterfaceController

class MyPresenter:
    def __init__(self, view):
        self.view = view
        self.controller = InterfaceController()
        self.view.actionTriggered.connect(self.onAction)

    def onAction(self, data):
        """Handle view action, call service, update view."""
        try:
            request = SNAPRequest(
                path="/myservice/operation",
                payload=MyInput(value=data["value"])
            )
            result = self.controller.executeRequest(request)
            self.view.showResult(result.result)
        except Exception as e:
            self.view.showError(str(e))
```

### Step 3: Add to Workflow
```python
# In main workflow manager or initialization
view = MyView()
presenter = MyPresenter(view)

# Add to workflow model chain
workflow_node = WorkflowNodeModel(
    view=view,
    continueAction=presenter.onAction,
    nextModel=next_node,
    name="My Step"
)
```

## Extending Existing Services

### Adding a New Operation to a Service

```python
# In an existing service class
@Register("/myservice/newoperation")
def newOperation(self, input_data: NewInput) -> NewOutput:
    # Implementation
    pass
```

### Modifying Data Structures

DAO objects are Pydantic models and support:
- **Field defaults**: `field: type = default_value`
- **Validators**: `@field_validator("field_name")`
- **Documentation**: Use docstrings and `Field(description="...")`

```python
from pydantic import BaseModel, Field, field_validator

class MyInput(BaseModel):
    """Input for my operation."""
    run_number: int = Field(..., description="Neutron run number")
    tolerance: float = Field(default=0.1, ge=0, le=1.0)

    @field_validator("run_number")
    @classmethod
    def validate_run(cls, v):
        if v <= 0:
            raise ValueError("Run number must be positive")
        return v
```

## Common Patterns

### Error Handling
```python
from snapred.backend.error.SnapredError import SnapredError

@Register("/service/operation")
def operation(self, data: Input) -> Output:
    try:
        result = process(data)
        return result
    except ValueError as e:
        raise SnapredError(f"Invalid data: {e}") from e
```

### Async Operations
```python
from snapred.ui.threading import worker_thread

@worker_thread
def long_operation(self, data):
    """Runs on background thread."""
    return expensive_computation(data)

# Called from presenter:
result = self.long_operation(data)  # Returns when complete
```

### Configuration Access
```python
from snapred.meta.Config import Config

class MyService(Service):
    @Register("/service/operation")
    def operation(self, data: Input) -> Output:
        timeout = Config["operations.timeout"]
        mode = Config["execution.mode"]
        # Use configuration
```

### Logging
```python
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)

@Register("/service/operation")
def operation(self, data: Input) -> Output:
    logger.info(f"Starting operation with {data}")
    try:
        result = process(data)
        logger.debug(f"Operation succeeded: {result}")
        return result
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        raise
```

## Testing Extensions

### Service Testing
```python
import pytest
from snapred.backend.service.MyService import MyService
from snapred.backend.dao.MyOperationRequest import MyInput

def test_operation():
    service = MyService()
    input_data = MyInput(run_number=123, parameter="test")
    result = service.performOperation(input_data)
    assert result.status == "success"
```

### Recipe Testing
```python
from snapred.backend.recipe.MyRecipe import MyRecipe, MyIngredients

def test_recipe_cooks():
    recipe = MyRecipe()
    ingredients = MyIngredients(workspace_name="test_ws")
    product = recipe.cook(ingredients)
    assert len(product.peak_positions) > 0
```

## Best Practices

1. **Type Safety**: Always use Pydantic models for data structures
2. **Single Responsibility**: Each service/recipe should have one purpose
3. **Error Handling**: Validate inputs and provide clear error messages
4. **Logging**: Log important operations for debugging
5. **Testing**: Write tests for services and recipes
6. **Documentation**: Document DAO fields and service paths
7. **Reusability**: Share code through utilities, not copy-paste
8. **Naming**: Use clear, descriptive names for services, methods, and fields

## Debugging

Enable debug logging in `Config`:
```python
Config["logging.level"] = "DEBUG"
```

Use entry/exit logging decorator:
```python
from snapred.meta.decorators.EntryExitLogger import EntryExitLogger

@EntryExitLogger()
def my_function(x):
    return x * 2
```

Breakpoint in services/recipes (they run synchronously):
```python
import pdb; pdb.set_trace()
```

Check Mantid workspaces:
```python
from snapred.backend.recipe.algorithm.Utensils import Utensils
utensils = Utensils()
utensils.PyInit()
ws = utensils.mtd("workspace_name")
print(ws.dataX(0)[:10])
```
