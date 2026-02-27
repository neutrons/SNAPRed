# Architecture Documentation Index

Welcome to SNAPRed's architecture documentation. Start here to understand how the system works and how to extend it.

## Quick Navigation

### For Understanding the System
- **[Architecture Overview](architecture.md)** - High-level system design, layers, and request-response flow
- **[Design Patterns](architecture.md#key-design-patterns)** - Service pattern, Recipe pattern, Decorators

### For Extending SNAPRed
- **[Extending SNAPRed](extending.md)** - Complete guide to adding new functionality
  - Creating new Services
  - Creating new Recipes
  - Adding UI Workflows
  - Common patterns and best practices

### Component-Specific Guides
- **[Backend Services Guide](backend_services.md)** - How services work, routing, using existing services
  - Core services (Reduction, Calibration, Normalization, etc.)
  - Service architecture and discovery
  - Working with Recipes
  - Error handling

- **[UI Architecture Guide](ui_architecture.md)** - Building user interfaces with PyQt5 and MVP pattern
  - Model-View-Presenter pattern
  - Creating Views and Presenters
  - Workflows and multi-step interactions
  - Custom widgets and threading

- **[Recipes and Algorithms Guide](recipes_algorithms.md)** - Data processing with Mantid
  - Recipe basics and patterns
  - Working with Mantid algorithms and workspaces
  - Complex multi-step workflows
  - Testing and best practices

## Learning Path

### If you're new to SNAPRed:
1. Read [Architecture Overview](architecture.md) for the big picture
2. Explore the codebase structure in `src/snapred/`
3. Pick a component to understand:
   - **Backend developer?** → [Backend Services Guide](backend_services.md) + [Recipes and Algorithms Guide](recipes_algorithms.md)
   - **UI developer?** → [UI Architecture Guide](ui_architecture.md)

### If you're adding a feature:
1. Determine the type of feature:
   - **New data processing?** → Create a Recipe (see [Recipes and Algorithms](recipes_algorithms.md))
   - **New business logic?** → Create a Service (see [Backend Services](backend_services.md))
   - **New user interaction?** → Create UI components (see [UI Architecture](ui_architecture.md))
2. Follow the step-by-step guides in [Extending SNAPRed](extending.md)
3. Review "Best Practices" and "Common Patterns" sections

### If you're debugging:
1. Understand the [request-response cycle](architecture.md#request-response-cycle)
2. Check the service/recipe involved ([Backend Services](backend_services.md))
3. Add logging and breakpoints as described in guides
4. Consult the "Debugging" sections in component guides

## Key Concepts

### Layers
```
UI Layer (PyQt5 Views + Presenters)
    ↓
Application Layer (Services + Orchestration)
    ↓
Data Processing Layer (Recipes + Mantid Algorithms)
    ↓
Infrastructure (Configuration, Logging, Error Handling)
```

### Request Flow
```
User Action → View Emits Signal → Presenter Creates SNAPRequest
    ↓
Service Orchestrates → Recipe Executes → Result Returned
    ↓
Presenter Updates View
```

### Type Safety
- All data structures use **Pydantic models** (DAOs in `backend/dao/`)
- Recipes are **Generic[Ingredients]** for type checking
- Services return **typed DAO objects**

### Design Patterns
- **@Singleton**: Services are single instances
- **@Register**: Methods registered as service endpoints
- **Model-View-Presenter**: UI follows MVP pattern
- **Recipe**: Generic data processors with Mantid algorithms

## File Organization

```
src/snapred/
├── backend/
│   ├── service/           ← Add new Service classes
│   ├── recipe/            ← Add new Recipe classes
│   ├── dao/              ← Data structure definitions
│   ├── api/              ← Request handling
│   ├── log/              ← Logging infrastructure
│   └── error/            ← Error types
└── ui/
    ├── presenter/        ← Add new Presenters
    ├── view/            ← Add new Views (PyQt5)
    ├── model/           ← UI-specific data
    ├── workflow/        ← Multi-step workflows
    ├── widget/          ← Reusable widgets
    ├── plotting/        ← Plotting utilities
    └── threading/       ← Background execution

meta/
├── decorators/          ← Service discovery, logging
├── validator/           ← Input validation
├── Config.py            ← Configuration system
└── redantic.py          ← Extended Pydantic
```

## Common Tasks

### Add a new REST endpoint
→ Create a Service method with `@Register` decorator ([Backend Services](backend_services.md))

### Add data processing algorithm
→ Create a Recipe in `backend/recipe/` ([Recipes and Algorithms](recipes_algorithms.md))

### Add UI screen
→ Create View + Presenter ([UI Architecture](ui_architecture.md))

### Connect UI to service
→ Create Presenter that calls service via `InterfaceController` ([UI Architecture](ui_architecture.md))

### Add a multi-step workflow
→ Link Views with `WorkflowNodeModel` ([UI Architecture - Workflow System](ui_architecture.md#workflow-system))

## Documentation Quick Links

| Component | Location | Key Concepts |
|-----------|----------|--------------|
| Services | `backend/service/` | Request routing, orchestration, singleton |
| Recipes | `backend/recipe/` | Type-safe processing, Mantid algorithms |
| Data Objects | `backend/dao/` | Pydantic validation, data contracts |
| UI Views | `ui/view/` | PyQt5 widgets, signal emission |
| UI Presenters | `ui/presenter/` | MVP logic, service calls |
| Config | `meta/Config.py` | Hierarchical configuration system |
| Logging | `backend/log/` | Structured logging, decorators |
| Errors | `backend/error/` | Exception hierarchy, error handling |

## Code Examples in Guides

Each guide includes complete, runnable examples:

- **architecture.md** - Request cycle, design patterns
- **extending.md** - Service, Recipe, UI workflow creation
- **backend_services.md** - Service routing, operation creation
- **ui_architecture.md** - View, Presenter, Workflow examples
- **recipes_algorithms.md** - Simple and complex recipe patterns

## Best Practices Summary

- ✅ Use Pydantic models for all data
- ✅ Validate inputs early
- ✅ Provide clear error messages
- ✅ Log significant operations
- ✅ Keep views logic-free (UI only)
- ✅ Separate concerns (View/Presenter/Service/Recipe)
- ✅ Test new components
- ✅ Document with examples

---

**Need help?** Start with the relevant guide above and look for "Best Practices" sections. Each guide has debugging tips at the end.
