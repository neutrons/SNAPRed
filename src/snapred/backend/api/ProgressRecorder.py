import datetime
from datetime import datetime, timedelta
from pathlib import Path
from pydantic import BaseModel

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty
from snapred.meta.decorators.Singleton import Singleton


class Step(BaseModel):
    # Class to holding information
    #   about how to calculate the execution-time estimate
    #    for a single workflow step.
    
    # name in <workflow> scope
    name: str
    
    # <referenceWorkspace>.getMemorySize() used as `N_ref`
    referenceWorkspace: str | None
    
    # Function to normalize `N_ref` to `N`: e.g. `N <- N_ref * log(N_ref)`.
    order: Callable[[float] float] = lambda N: N


class _Measurement(BaseModel):
    # Post-execution timing data from a specific workflow step.
    dt: timedelta
    N_ref: float | None


class _Estimate(BaseModel):
    # Estimated time for a specific workflow step.
    dt: timedelta
    
    # Mean value of order-normalized N_ref:
    N: float | None = None
    
    # Number of records contributing to the running mean.
    count: int | None = None

    @classmethod
    def default(cls) -> "Estimate":
        # Don't use default attributes, as this method
        #   may eventually depend on arguments.
        return Estimate(timedelta(minutes=2.0))
    
    def accumulate(self, measurement: _Measurement, order: Callable[[float], float]):
        next_N = measurement.N_ref
        return Estimate_(
          == we are here==
        )
    
class _ProgressStep(BaseModel):
    # How to calculate the execution-time estimate
    details: Step
    
    # Post-execution timing data for the step.
    records: List[_Measurement]
    
    # Time estimate for the step.
    estimate: _Estimate
    

class ProgressRecorder(BaseModel):

    # Map from <workflow name>s to progress steps:
    #   the order of the steps in the list must correspond to the recording order
    #     for the steps in each workflow.
    stepss: Dict[str, List[_ProgressStep]] = {}

    def __init__(self):
        super().__init__()
        
        # Assume that there are no _nested_ workflows!
        self._workflowStartTime = None
        self._stepStartTime = None
        
        self.mantidSnapper = MantidSnapper(parentAlgorithm=None, name=self.__class__.__name__)
        atexit.register(ProgressRecorder._unloadResident, self)

    @staticmethod
    def _unloadResident(instance):
        # Unload method to register with `atexit`.
        LocalDataService().writeProgressRecords(instance)      

    def _start(self) -> datetime:
        self._startNextStep()
        self._workflowStartTime = self._stepStartTime
        return self._workflowStartTime
    
    def _end(self):
        pass

    def _startNextStep(self) -> datetime:
        self._stepStartTime = datetime.now().astimezone()
        return self._stepStartTime
               
    def register(self, workflowName: str, steps: List[Step]):
        # Register a list of progress-recording steps, 
        #   and where possible, not overwrite any existing information.
        
        workflow_steps = self.stepss.get(workflowName, {})
        # Add the default "start" and "end" steps.
        if "start" not in workflow_steps:
            _step = Step(name="start")
            workflow_steps["start"] = _ProgressStep(
                details=_step
                records=[],
                estimate=_Estimate.default()
            )
        if "end" not in workflow_steps:
            _step = Step(name="end")
            workflow_steps["end"] = _ProgressStep(
                details=_step,
                records=[],
                estimate=_Estimate.default()
            )
       
        for step in steps:
            _step = workflow_steps.get(step.name)
            if bool(_step) and _step == step:
                # Step details haven't changed:
                #   use the existing execution-time data.
                continue
            
            # Initialize a new version of the step.
            workflow_steps[step.name] = _ProgressStep(
                details=step,
                records=[],
                estimate=_Estimate.default()
            )

        self.stepss[workflowName] = workflow_steps
            
    def record(self, workflowInstance, stepName: str):
        # Validate the workflow and step registration.
        workflowName = workflowInstance.__class__.__name__
        if workflowName not in self.stepss:
            raise RuntimeError(f"Unregistered workflow: '{workflowName}'.")
        step = self.stepss[workflowName].get(stepName)
        if step is None:
            raise RuntimeError(f"Workflow '{workflowName}': unregistered step '{stepName}'.")

        # Get the elapsed time, and start timing the next step.
        startStep = self._startStepTime
        elapsed = self._startNextStep() - startStep
        
        N_ref = None
        if self.mantidSnapper.mtd.doesExist(step.referenceWorkspace):
            N_ref = self.mantidSnapper.mtd[step.referenceWorkspace].getMemorySize()
        measurement =  _Measurement(
            dt=elapsed,
            N_ref=N_ref
        ) 
        step.records.append(measurement)
        
