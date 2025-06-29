import datetime
from datetime import datetime, timedelta
from enum import Enum
from functools import cached_property
import numpy as np
from pathlib import Path
from pydantic import BaseModel
from scipy.interpolate import BSpline, make_splrep
from typing import ClassVar, Self

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty, cached_classproperty
from snapred.meta.decorators.Singleton import Singleton

class ComputationOrder(str, Enum):
    
    O_LOGN  = "O_LOGN"
    O_N     = "O_N"
    O_NLOGN = "O_NLOGN"
    O_N_2   = "O_N_2"
    O_N_3   = "O_N_3"
     
    def __call__(self, N_ref: float) -> float:
        N = None
        match self:
            case O_LOGN:
                N = np.log(N_ref)
            case O_N:
                N = N_ref
            case O_NLOGN:
                N = N_ref * np.log(N_ref)
            case O_N_2:
                N = N_ref**(2.0)
            case O_N_3:
                N = N_ref**(3.0)
            case _:
                raise RuntimeError(f"unrecognized `ComputationalOrder`: {self}")
        return N


class Step(BaseModel):
    # Class to holding information
    #   about how to calculate the execution-time estimate
    #    for a single workflow step.
    
    # name in <workflow> scope
    name: str
    
    # float(<referenceWorkspace>.getMemorySize()) is used as `N_ref`
    referenceWorkspace: str | None = None
    
    # float(<path>.stat().st_size) is used as `N_ref`
    referencePath: Path | None = None
    
    # Normalization of `N_ref`, from the as-saved measurement, to `N`, as used by
    #   the spline fit.
    order: ComputationalOrder = ComputationalOrder.O_N
    
    def N_ref(self) -> float:
        # Access the reference value
        N_ref = None
        if self.referenceWorkspace is not None:
            if self.mantidSnapper.mtd.doesExist(self.referenceWorkspace):
                N_ref = float(self.mantidSnapper.mtd[self.referenceWorkspace].getMemorySize())
        elif self.referencePath is not None:
            if self.referencePath.exists():
                N_ref = float(self.referencePath.stat().st_size)
        return N_ref

class _Measurement(BaseModel):
    # Post-execution timing data from a specific workflow step.
    dt: float # seconds
    
    # Non-normalized reference value:
    #   only the original value should be saved, which is the data point itself.
    # In case there's no `N_ref`, this `_Measurement` only includes timing data.
    N_ref: float | None


class _Estimate(BaseModel):
    # (<Number of distinct contributing `_Measurement` records>,
    #    <Total number of contributing records>)
    count: Tuple[int, int] | None = None

    # Spline args from most recent "update" calculation.
    tck: Tuple[List[float], List[float], int] = None

    _default: ClassVar[Self] = None
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.tck is not None:
            self._spl = BSpline(*self.tck, extrapolate=True)

    @classproperty
    def SPLINE_ORDER(cls):
        return Config["workflows_data.timing.spline_order"]
            
    @classproperty
    def default(cls) -> Self:
        # We don't use default attributes, as this method
        #   may eventually depend on arguments.
        
        # Implementation notes:
        # * This caches the `_default` value as a class attribute.
        # * TODO: Attempting a `cached_classproperty` implementation had some 
        #   issues that need to be resolved.
        
        if cls._default is None:
            # 60.0s to process 1GB of data.
            Ns = np.linspace(0.0, 1.0e9, self.SPLINE_ORDER + 1)
            dts = np.linspace(0.0, 60.0, self.SPLINE_ORDER + 1)
            cls._default = _Estimate()
            cls._default.update(Ns, dts, ComputationalOrder.O_N)
        return cls._default
                    
    # Estimate time for a specific workflow step.
    def dt(self, N) -> timedelta:
        # Estimate dt from normalized 'N'.
        if self._spl is None:
            raise RuntimeError("Usage error: `dt` called before `update`.")
        return float(self._spl(N))

    def update(self, measurements: List[_Measurement], order: Callable[[float], float]):
        # Update the spline model of the time dependence.
        Ns, dts = self._prepareData(measurements, order)
        if len(Ns) > self.SPLINE_ORDER + 1:
            # retain counts contributing to the estimate: (|<distinct points>|, |<all points>|)
            self.count = (len(Ns), len(measurements))
            
            self._spl = make_splrep(Ns, dts, k=self.SPLINE_ORDER)
            self.tck = (list(spl.tck[0]), list(spl.tck[1]), spl.tck[2])

    def _prepareData(self, measurements: List[_Measurement], order: Callable[[float], float]) -> Tuple[List[float], List[float]]:        
        # Sort and accumulate the measurement data.
        ps = sorted([(order(measurement.N_ref), measurement.dt) for measurement in measurements], key=lambda t: t[0])
        Ns, dts = unzip(self._accumulateDuplicates(ps))
        return Ns, dts
            
    def _accumulateDuplicates(self, ps: List[Tuple(float, float)]) -> List[Tuple(float, float)]:
        # Accumulate adjacent measurements with the same N values;
        #   as part of the fitting process, this is applied _after_ the computational-order normalization.
        ps_ = []
        p_prev = None
        count = 0
        for p in ps:
            n, dt = p
            if p_prev is not None:
                n_prev, dt_prev = p_prev
                if np.isclose(n, n_prev):
                    # accumulate
                    dt_prev += dt
                    count += 1
                elif count > 1:
                    # take the mean value
                    dt_prev /= count
                    count = 0
                    continue
            ps_.append(p)
            count = 1
            p_prev = p
        return ps_

    
class _ProgressStep(BaseModel):
    # Information about how to calculate the execution-time estimate
    details: Step
    
    # Post-execution timing data for the step.
    measurements: List[_Measurement]
    
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
        self.currentStep = None
        
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
        #   and where possible, do not overwrite existing information.
        
        workflow_steps = self.stepss.get(workflowName, {})
        # Add the default "start" and "end" steps.
        if "start" not in workflow_steps:
            _step = Step(name="start")
            workflow_steps["start"] = _ProgressStep(
                details=_step
                measurements=[],
                estimate=_Estimate.default
            )
        if "end" not in workflow_steps:
            _step = Step(name="end")
            workflow_steps["end"] = _ProgressStep(
                details=_step,
                measurements=[],
                estimate=_Estimate.default
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
                measurements=[],
                estimate=_Estimate.default
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
        elapsed = float((self._startNextStep() - startStep).total_seconds())

        # FINALIZE the measurement data for the previous step.
        if self.currentStep is not None:
            # Compute the required reference value.
            N_ref = self.currentStep.N_ref()

            # Record the execution-time measurement
            self.currentStep.measurements.append(
                _Measurement(dt=elapsed, N_ref=N_ref)
            )
        
            # Restrict the maximum length of the measurements lists.
            max_measurements = Config["workflows_data.timing.max_measurments"]
            if len(self.currentStep.measurements) > max_measurements:
                # Forget the oldest
                self.currentStep.measurements = self.currentStep.measurements[-max_measurements:]
         
            # If necessary, update the estimate for the previous step.
            estimated_dt = self.currentStep.estimate.dt(self.currentStep.details.order(N_ref))
            if abs(estimated_dt - elapsed) / elapsed > Config["workflows_data.timing.update_threshold"]:
                self.currentStep.estimate.update(self.currentStep.measurements, self.currentStep.details.order)
                
        # Go to the next step.
        self.currentStep = step

    def stepTimeRemaining(self) -> float:
        # Estimate the time remaining for the current step.
        if self.currentStep is None:
            raise RuntimeError("No workflow step is in progress.") 
    
        elapsed = float((self._startStepTime - datetime.now().astimezone()).total_seconds())
        
        N_ref = self.currentStep.N_ref()
        remainder = self.currentStep.estimate.dt(self.currentStep.details.order(N_ref)) - elapsed
        if remainder < 0.0:
            remainder = 0.0
        return remainder


    def workflowTimeRemaining(self, workflowInstance) -> float:
        # Estimate the time remaining for the current workflow.
        # (This will only be accurate for the case where all reference-workspaces or paths
        #  are available at the time of this call!)

        # Validate the workflow registration.
        workflowName = workflowInstance.__class__.__name__
        if workflowName not in self.stepss:
            raise RuntimeError(f"Unregistered workflow: '{workflowName}'.")
        steps = self.stepss[workflowName]
        
        default_N_ref = 1.0e9 # use when actual N_ref for a step doesn't exist yet
        now = datetime.now().astimezone()
        start = self._workflowStartTime if bool(self._workflowStartTime) else now
        elapsed = float((now - start).total_seconds())
        
        total = 0.0
        for step in steps:
            N_ref = step.N_ref()
            if N_ref is None:
                N_ref = default_N_ref
            total += step.estimate.dt(step.details.order(N_ref))
        
        remainder = total - elapsed
        if remainder < 0.0:
            remainder = 0.0
        return remainder 
