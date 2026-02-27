# SNAPRed Architecture

## Overview

SNAPRed is a desktop application for lifecycle management of neutron diffraction data from the SNAP instrument. The architecture follows a layered approach with clear separation between UI, backend services, and data processing.

**Core components:**
- **Backend**: Service orchestration, data access, recipes for data processing
- **UI**: PyQt5-based interface with presenter/model/view pattern
- **Meta**: Utilities, decorators, validators, and configuration

## Architecture Layers

### 1. Presentation Layer (`snapred/ui/`)
Handles all user interface concerns using PyQt5 and the Model-View-Presenter pattern.

**Key components:**
- `view/` - PyQt5 widgets and UI layouts
- `presenter/` - Presenters that connect views to business logic
- `model/` - UI-specific data models
- `handler/` - Event handlers and UI callbacks
- `workflow/` - Multi-step workflow management
- `widget/` - Reusable custom widgets
- `plotting/` - Matplotlib/Mantid plotting utilities
- `threading/` - Async task execution

**Typical flow**: User action → Handler → Presenter → Service

### 2. Application Layer (`snapred/backend/`)
Coordinates requests, manages services, and orchestrates recipes.

**Key components:**
- `api/` - Request handling and interface control
- `service/` - Business logic services (reduction, calibration, normalization)
- `recipe/` - Data processing workflows (Mantid-based algorithms)
- `dao/` - Data access objects (SNAPRequest, data structures)
- `log/` - Logging infrastructure
- `error/` - Error handling and exceptions

**Request flow**: API receives `SNAPRequest` → Service orchestrates → Recipe executes

### 3. Data Processing Layer (`snapred/backend/recipe/`)
Wraps Mantid algorithms in composable Recipe objects.

**Key concepts:**
- `Recipe` - Base class for data transformations with type-safe ingredients
- `GenericRecipe` - Standard pattern for multi-algorithm workflows
- `Utensils` - Holds Mantid algorithm references and workspace manager
- `Algorithm` - Individual computational steps

### 4. Meta Layer (`snapred/meta/`)
Cross-cutting utilities and infrastructure.

**Key components:**
- `decorators/` - Service discovery, logging, exception handling
- `validator/` - Input validation (run numbers, paths)
- `builder/` - Object builders (grocery lists, configurations)
- `Config.py` - Application configuration
- `redantic.py` - Extended Pydantic models for type safety

## Request-Response Cycle

```
User UI Action
    ↓
Handler creates SNAPRequest(path="/service/operation", payload=data)
    ↓
ApiService.executeRequest(request)
    ↓
ServiceDirectory routes to appropriate Service
    ↓
Service.orchestrateRecipe(request)
    ↓
Recipe processes ingredients through algorithms
    ↓
Result returned through response chain
    ↓
Presenter updates View
```

## Key Design Patterns

### Service Pattern
Services are singletons (via `@Singleton` decorator) that:
- Register operation paths using `@Register` decorator
- Accept `SNAPRequest` objects with path-based routing
- Return typed results

```python
@Singleton
class MyService(Service):
    @Register("/myservice/process")
    def process(self, ingredients: MyIngredients) -> MyResult:
        # Implementation
        pass
    
    def name(self):
        return "MyService"
```

### Recipe Pattern
Recipes are generic, type-safe processors using Pydantic for validation:

```python
class MyRecipe(Recipe[MyIngredients]):
    def cook(self, ingredients: MyIngredients) -> MyResult:
        # Access Mantid algorithms via self.utensils
        # Execute transformations
        # Return results
        pass
```

### Decorator Pattern
Framework provides decorators for common concerns:
- `@Singleton` - Single instance per application
- `@Register(path)` - Path registration for routing
- `@EntryExitLogger` - Automatic logging
- `@ExceptionHandler` - Error handling
- `@Builder` - Object construction

## Data Structures

### SNAPRequest
Core message type for all backend operations:
```python
class SNAPRequest(BaseModel):
    path: str                              # "/service/operation"
    payload: Optional[Any] = None          # Operation-specific data
    hooks: Optional[dict[str, List[Hook]]] = None  # Callbacks
```

### DAO (Data Access Objects)
Located in `backend/dao/`, these Pydantic models ensure type safety:
- Define data contracts
- Provide validation
- Bridge between layers

## Service Architecture

### Core Services
- **ReductionService** - Data reduction workflows
- **CalibrationService** - Calibration procedures
- **NormalizationService** - Data normalization
- **LiteDataService** - Data access and caching
- **StateIdLookupService** - Instrument state management

### Service Discovery
- `ServiceDirectory` - Central registry of all services
- `ServiceFactory` - Creates and configures services
- Path-based routing enables loose coupling

## Configuration

Configuration uses a hierarchical system in `snapred/meta/Config.py`:
- Environment variables
- Configuration files (YAML)
- Runtime overrides
- Accessed via `Config["key.path.notation"]`

## Workspace Management

The Mantid `Workspace` is the central data structure:
- Holds experimental data and metadata
- Named and referenced by path
- Managed through `MantidSnapper` (Mantid wrapper)
- Generated names via `WorkspaceNameGenerator`

## Error Handling

### Exception Hierarchy
- `SnapredError` - Base exception
- `ContinueWarning` - Recoverable errors (user can continue)
- `DatasourceError` - Data access failures
- Logged and reported back to UI

### Hook System
Requests can specify hooks (callbacks) for:
- Error handling
- Completion notifications
- Data streaming
- Progress updates

## Extensibility Points

### Adding a New Service
1. Create class extending `Service`
2. Use `@Singleton` decorator
3. Register operations with `@Register` decorator
4. Return typed DAO objects

### Adding a Recipe
1. Create class extending `Recipe[IngredientType]`
2. Implement `cook()` method
3. Use `self.utensils` for Mantid algorithms
4. Validate inputs via Pydantic

### Adding UI Workflow
1. Create view (QWidget subclass)
2. Create presenter connecting view to service
3. Create workflow model linking presenters
4. Register in workflow manager

## Threading Model

UI operations run on Qt main thread. Long-running operations:
- Use `QThread` via `threading/` utilities
- Services execute synchronously
- Results marshaled back to UI thread
- Hooks enable async response handling

## Logging

Structured logging via `snapredLogger`:
- Decorator-based automatic logging
- Entry/exit point tracking
- Exception logging
- File and console output
- Configurable levels
