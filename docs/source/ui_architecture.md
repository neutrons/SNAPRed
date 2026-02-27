# UI Architecture Guide

This guide explains SNAPRed's user interface architecture and how to build UI components.

## Architecture Overview

SNAPRed uses a **Model-View-Presenter (MVP)** pattern for UI development with PyQt5.

```
User Input
    ↓
View (PyQt5 Widget)
    ↓
Presenter (Logic Handler)
    ↓
Service (Business Logic)
    ↓
Recipe (Data Processing)
    ↓
Result
    ↓
Presenter → View (Update Display)
```

**Separation of concerns:**
- **View**: Rendering and user input (PyQt5)
- **Presenter**: Connects view to services
- **Model**: UI-specific data (separate from backend DAOs)
- **Service**: Business logic

## Directory Structure

```
ui/
├── view/          # PyQt5 widgets and layouts
├── presenter/     # MVP presenters
├── model/         # UI-specific data models
├── handler/       # Event handlers
├── workflow/      # Multi-step workflows
├── widget/        # Reusable custom widgets
├── plotting/      # Plotting utilities
└── threading/     # Async task execution
```

## Creating a UI Component

### Step 1: Create a View (PyQt5 Widget)

```python
# ui/view/MyView.py
from qtpy import QtWidgets, QtCore

class MyView(QtWidgets.QWidget):
    """A simple input/output view."""

    # Signals for user actions
    inputChanged = QtCore.Signal(str)
    processRequested = QtCore.Signal(dict)

    def __init__(self):
        super().__init__()
        self.setupUI()

    def setupUI(self):
        """Build the UI layout."""
        layout = QtWidgets.QVBoxLayout()

        # Input section
        input_label = QtWidgets.QLabel("Input Value:")
        self.input_field = QtWidgets.QLineEdit()
        self.input_field.textChanged.connect(self.onInputChanged)

        # Button
        self.process_button = QtWidgets.QPushButton("Process")
        self.process_button.clicked.connect(self.onProcessClicked)

        # Output
        self.result_display = QtWidgets.QTextEdit()
        self.result_display.setReadOnly(True)

        # Assemble layout
        layout.addWidget(input_label)
        layout.addWidget(self.input_field)
        layout.addWidget(self.process_button)
        layout.addWidget(QtWidgets.QLabel("Result:"))
        layout.addWidget(self.result_display)

        self.setLayout(layout)

    def onInputChanged(self, text):
        """Called when input changes."""
        self.inputChanged.emit(text)

    def onProcessClicked(self):
        """Called when process button clicked."""
        data = {"value": self.input_field.text()}
        self.processRequested.emit(data)

    def displayResult(self, result):
        """Update view with result."""
        self.result_display.setText(str(result))

    def showError(self, message):
        """Display error to user."""
        QtWidgets.QMessageBox.critical(self, "Error", message)

    def enableProcessing(self, enabled: bool):
        """Enable/disable processing UI."""
        self.process_button.setEnabled(enabled)
        self.input_field.setEnabled(enabled)
```

**View best practices:**
- Emit signals for user actions (don't call business logic directly)
- Keep view logic minimal (calculation-free)
- Provide methods for presenter to update display
- Make views reusable and configurable

### Step 2: Create a Presenter

```python
# ui/presenter/MyPresenter.py
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.api.InterfaceController import InterfaceController
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)

class MyPresenter:
    """Connects MyView to business logic."""

    def __init__(self, view):
        self.view = view
        self.controller = InterfaceController()

        # Connect view signals to presenter methods
        self.view.processRequested.connect(self.onProcessRequested)
        self.view.inputChanged.connect(self.onInputChanged)

    def onInputChanged(self, text):
        """Validate input as user types."""
        if len(text) > 0:
            self.view.enableProcessing(True)
        else:
            self.view.enableProcessing(False)

    def onProcessRequested(self, data):
        """Handle process request from view."""
        logger.info(f"Processing: {data}")

        # Disable UI during processing
        self.view.enableProcessing(False)

        try:
            # Create request
            from snapred.backend.dao.MyOperationRequest import MyInput
            input_data = MyInput(value=data["value"])

            request = SNAPRequest(
                path="/myservice/operation",
                payload=input_data
            )

            # Execute request
            result = self.controller.executeRequest(request)

            # Update view with result
            self.view.displayResult(result)
            logger.info("Processing completed successfully")

        except Exception as e:
            logger.error(f"Processing failed: {e}")
            self.view.showError(str(e))

        finally:
            self.view.enableProcessing(True)
```

**Presenter best practices:**
- Validation and transformation logic
- Error handling and user feedback
- Service calls via InterfaceController
- Update view with results

### Step 3: Use the Component

In a workflow or main window:

```python
# In main application or workflow manager
from ui.view.MyView import MyView
from ui.presenter.MyPresenter import MyPresenter

# Create view and presenter
view = MyView()
presenter = MyPresenter(view)

# Show view
view.show()

# Or add to workflow
workflow_node = WorkflowNodeModel(
    view=view,
    continueAction=presenter.onProcessRequested,
    nextModel=next_step,
    name="My Processing Step"
)
```

## Workflow System

Multi-step workflows are built with `WorkflowNodeModel`.

### Creating a Workflow

```python
from snapred.ui.model.WorkflowNodeModel import WorkflowNodeModel

# Step 1
view1 = MyView1()
presenter1 = MyPresenter1(view1)

# Step 2
view2 = MyView2()
presenter2 = MyPresenter2(view2)

# Step 3
view3 = MyView3()
presenter3 = MyPresenter3(view3)

# Link steps together
node3 = WorkflowNodeModel(
    view=view3,
    continueAction=presenter3.onContinue,
    nextModel=None,  # Last step
    name="Final Step"
)

node2 = WorkflowNodeModel(
    view=view2,
    continueAction=presenter2.onContinue,
    nextModel=node3,
    name="Processing Step"
)

node1 = WorkflowNodeModel(
    view=view1,
    continueAction=presenter1.onContinue,
    nextModel=node2,
    name="Input Step"
)

# Use workflow
for node in node1:  # Iterates through linked nodes
    # Display each node's view
    # Wait for user
    # Call continueAction when done
```

### Workflow Properties

```python
WorkflowNodeModel(
    view=my_view,                    # PyQt5 widget
    continueAction=presenter.onContinue,  # Callable when continuing
    nextModel=next_node,              # Next workflow node
    name="Step Name",                 # Display name
    required=True,                    # Skip if False
    iterate=False,                    # Repeat step if True
    continueAnywayHandler=handler     # Handle ContinueWarning
)
```

## Custom Widgets

Create reusable widgets in `ui/widget/`.

```python
# ui/widget/RunNumberInput.py
from qtpy import QtWidgets, QtCore

class RunNumberInput(QtWidgets.QWidget):
    """Input widget for run numbers."""

    valueChanged = QtCore.Signal(int)

    def __init__(self):
        super().__init__()
        layout = QtWidgets.QHBoxLayout()

        label = QtWidgets.QLabel("Run Number:")
        self.spinbox = QtWidgets.QSpinBox()
        self.spinbox.setMinimum(1)
        self.spinbox.setMaximum(999999)
        self.spinbox.valueChanged.connect(self.valueChanged.emit)

        layout.addWidget(label)
        layout.addWidget(self.spinbox)
        self.setLayout(layout)

    def getValue(self):
        return self.spinbox.value()

    def setValue(self, value):
        self.spinbox.setValue(value)
```

Use custom widgets:
```python
from ui.widget.RunNumberInput import RunNumberInput

class MyView(QtWidgets.QWidget):
    def setupUI(self):
        self.run_input = RunNumberInput()
        # Connect and use
```

## Plotting

Use Mantid plotting utilities in `ui/plotting/`.

```python
# In a view or presenter
from snapred.ui.plotting.MantidPlotter import MantidPlotter

plotter = MantidPlotter()

# Plot workspace
canvas = plotter.plotWorkspace(
    workspace_name="my_data",
    spectrum_numbers=[0, 1, 2]
)

# Add to view
layout.addWidget(canvas)
```

## Threading

Long-running operations should run in background threads.

```python
from snapred.ui.threading import worker_thread
from snapred.backend.api.InterfaceController import InterfaceController

class MyPresenter:
    @worker_thread
    def runLongOperation(self, data):
        """Runs on background thread."""
        controller = InterfaceController()
        request = SNAPRequest(
            path="/service/operation",
            payload=data
        )
        return controller.executeRequest(request)

    def startLongOperation(self):
        """Called from UI thread."""
        self.view.showProgress("Processing...")

        # This returns immediately; result comes via signal
        result = self.runLongOperation(self.data)

        # When done, update UI
        self.view.showResult(result)
```

## Models (UI-Specific)

UI models store state for views, separate from backend DAOs.

```python
# ui/model/MyModel.py
from dataclasses import dataclass

@dataclass
class MyModel:
    """UI state for MyView."""
    input_value: str = ""
    processing: bool = False
    result: str = ""
    error: str = ""
```

In presenter:

```python
class MyPresenter:
    def __init__(self, view):
        self.view = view
        self.model = MyModel()

    def onProcessRequested(self, data):
        self.model.processing = True
        self.model.input_value = data["value"]
        # Process
```

## Signals and Slots

PyQt5 uses signals/slots for event handling.

```python
class MyView(QtWidgets.QWidget):
    # Define signals
    dataReady = QtCore.Signal(object)  # Emits object
    userAction = QtCore.Signal()       # No data

    def emitSignal(self):
        self.dataReady.emit({"key": "value"})

# Connect signal to slot
view.dataReady.connect(presenter.onDataReady)

# Slot method (any callable)
def onDataReady(self, data):
    print(data["key"])
```

## Dialog Usage

```python
from qtpy import QtWidgets

# File dialog
filename, _ = QtWidgets.QFileDialog.getOpenFileName(
    self, "Open File", "", "Data Files (*.txt)"
)

# Message box
QtWidgets.QMessageBox.information(
    self, "Title", "Message"
)

# Progress dialog
progress = QtWidgets.QProgressDialog(
    "Processing...", "Cancel", 0, 100
)
progress.setValue(50)
```

## Best Practices

1. **Separation**: Views don't know about services
2. **Signals**: Use Qt signals for view-to-presenter communication
3. **Error Handling**: Catch errors in presenters, show in views
4. **Threading**: Keep UI responsive with background threads
5. **Testing**: Test views with mock presenters
6. **Documentation**: Document view parameters and signals
7. **Consistency**: Follow existing patterns and naming
8. **Reusability**: Extract common widgets and presenters

## Example: Complete Component

```python
# View
class SearchView(QtWidgets.QWidget):
    searchRequested = QtCore.Signal(str)

    def setupUI(self):
        layout = QtWidgets.QVBoxLayout()
        self.search_input = QtWidgets.QLineEdit()
        search_btn = QtWidgets.QPushButton("Search")
        search_btn.clicked.connect(self.onSearch)
        self.results = QtWidgets.QTextEdit()
        self.results.setReadOnly(True)

        layout.addWidget(QtWidgets.QLabel("Search:"))
        layout.addWidget(self.search_input)
        layout.addWidget(search_btn)
        layout.addWidget(self.results)
        self.setLayout(layout)

    def onSearch(self):
        query = self.search_input.text()
        self.searchRequested.emit(query)

    def displayResults(self, results):
        self.results.setText(str(results))

# Presenter
class SearchPresenter:
    def __init__(self, view):
        self.view = view
        self.controller = InterfaceController()
        self.view.searchRequested.connect(self.onSearch)

    def onSearch(self, query):
        request = SNAPRequest(
            path="/search/find",
            payload={"query": query}
        )
        result = self.controller.executeRequest(request)
        self.view.displayResults(result)

# Usage
view = SearchView()
presenter = SearchPresenter(view)
view.show()
```
