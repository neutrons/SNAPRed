WorkflowBuilder Class Documentation
===================================

.. module:: WorkflowBuilder
   :synopsis: A class for building and managing a workflow with user-defined steps.

Overview
--------

The `WorkflowBuilder` class facilitates the creation and management of a multi-step workflow with customizable steps. Each step of the workflow is associated with a "continue" action and a graphical user interface (GUI) view.
This class abstracts the complexity of handling transitions between workflow steps and provides an intuitive way to construct workflows by sequentially adding steps.

Creating Workflow Steps
-----------------------

Each step of the workflow is added using the `addNode` method. The method takes the following parameters:

- `continueAction`: A callable representing the action to be performed when the "continue" button is clicked for that step.
- `subview`: A widget representing the GUI element of the step. This view is typically displayed to the user to gather input or display information.
- `name`: (Optional) A string describing the name or purpose of the step. This name serves as an identifier for the step and is displayed to the user.

The `continueAction` parameter defines the behavior to execute when the "continue" button associated with the step is clicked.
This allows custom actions to be executed before transitioning to the next step.

Building and Executing the Workflow
-----------------------------------

After adding all the desired steps using `addNode`, the workflow can be built using the `build` method, which returns a `Workflow` instance encapsulating the configured workflow.

The resulting workflow can then be executed using various methods provided by the `Workflow` class, including starting the workflow and navigating through its steps.

Creating Workflow Widgets Easily
---------------------------------

To create workflow widgets easily, follow these steps:

1. Import the necessary classes:

.. code-block:: python

   from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder

2. Define the "continue" action for each step as a callable function that performs the required logic when the step is completed.

3. Create the subview widgets for each step. These widgets can be existing GUI elements or custom views designed for each step.

4. Instantiate a `WorkflowBuilder` instance and use the `addNode` method to add steps to the workflow. Provide the "continue" action and subview widget for each step.

5. Build the workflow using the `build` method.

6. Execute the workflow using the provided methods to guide users through the defined steps.

Example Usage
-------------

Here's a simple example of how the `WorkflowBuilder` class can be used to create and manage a multi-step workflow with custom steps:

.. code-block:: python
    from snapred.ui.workflow.WorkflowBuilder import WorkflowBuilder

    class WorkflowExample(QWidget):
        def __init__(self):
            self.workflow = (
                WorkflowBuilder(parent=self)
                .addNode(self.calibration_reduction_action, "Calibrating")
                .addNode(self.assess_calibration_action, "Assessing")
                .addNode(self.save_calibration_action, "Saving")
                .build()
            )
            self.responses = {}

        def calibration_reduction_action(self, workflowPresenter):
            view = workflowPresenter.widget.tabView
            # pull field from view
            inputs = view.getFieldText("inputs")

            print("Calibrating with inputs:", inputs)
            self.responses['calibration'] = {"runNumber": "123"}

        def assess_calibration_action(self, workflowPresenter):
            view = workflowPresenter.widget.tabView
            # pull field from view
            inputs = view.getFieldText("inputs")
            print("Assessing calibration with inputs:", inputs)
            previous_response = self.responses.get('calibration', {})
            self.responses['assessment'] = {"assessmentResult": "Pass", **previous_response}

        def save_calibration_action(self, workflowPresenter):
            view = workflowPresenter.widget.tabView
            # pull field from view
            inputs = view.getFieldText("inputs")
            print("Saving calibration with inputs:", inputs)
            previous_response = self.responses.get('assessment', {})
            # Perform saving logic using previous_response

        @property
        def widget(self):
            return self.workflow.presenter.widget

    # Create an instance of the WorkflowExample class
    example = WorkflowExample()
    self.layout.addWidget(example.widget)
