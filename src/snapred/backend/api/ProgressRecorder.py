import atexit
from datetime import datetime, timedelta, timezone
from enum import StrEnum
import functools
from hashlib import sha256
import numpy as np
from pathlib import Path
from pydantic import BaseModel
from scipy.interpolate import BSpline, make_splrep
from types import FrameType, FunctionType, MethodType
from typing import Any, Callable, ClassVar, Dict, List, Self, Tuple

from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.recipe.GenericRecipe import GenericRecipe
from snapred.backend.recipe.Recipe import Recipe
from snapred.backend.service.Service import Service

from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty
from snapred.meta.decorators.Singleton import Singleton

class ComputationalOrder(StrEnum):
    
    O_0     = "O_0"
    O_LOGN  = "O_LOGN"
    O_N     = "O_N"
    O_NLOGN = "O_NLOGN"
    O_N_2   = "O_N_2"
    O_N_3   = "O_N_3"
     
    def __call__(self, N_ref: float) -> float:
        # Incoming 'N_ref' must always have a value:
        #   even in the case of `O_0`.
        if N_ref is None:
            raise RuntimeError("Usage error: incoming 'N_ref' must have a value.")
        N = None
        match self:
            case Self.O_0:
                N = 1.0
            case Self.O_LOGN:
                N = np.log(N_ref)
            case Self.O_N:
                N = N_ref
            case Self.O_NLOGN:
                N = N_ref * np.log(N_ref)
            case Self.O_N_2:
                N = N_ref**(2.0)
            case Self.O_N_3:
                N = N_ref**(3.0)
            case _:
                raise RuntimeError(f"unrecognized `ComputationalOrder`: {self}")
        return N


class _Step(BaseModel):
    # Class holding details
    #   about how to calculate the execution-time estimate
    #    for a process (either workflow or recipe) step.
    
    # The key used to uniquely identify this process step.
    # At present this is `(<module name> or <file name>, <class name>, <method name> [, <step name>])`. 
    key: Tuple[str | None, ...]

    # Hex digest of `_N_ref` `Callable`:
    #   this allows accounting for changes in how `N_ref` is computed,
    #   but doesn't have the problems associated with making an
    #   `N_ref`-callable persistent.
    N_ref_hash: str
    """
      == In case of change, don't delete the measurements!  Just re-initialize the `_Estimate`!
    """
    
    # Standard computational-order is used to normalize `N_ref`, from the as-saved measurement,
    #   to `N`, as used by the estimate's spline fit.
    order: ComputationalOrder = ComputationalOrder.O_0
    
    def __init__(
            self, *,
            key: Tuple[str | None, ...],
            # Default values compute constant-time estimate.
            N_ref: Callable[..., float] = lambda *_args, **_kwargs: 1.0,
            N_ref_args: Tuple[Tuple[Any,...], Dict[str, Any]] = ((), {}),
            order: ComputationalOrder=ComputationalOrder.O_0
        ):
        # The persistent part of the `Step`, as the Pydantic model,
        #   retains the hash digest of the `N_ref`-callable, but not the callable itself.
        N_ref_hash = _Step._callable_hash(N_ref)
        super().__init__(
            key=key,
            N_ref_hash=N_ref_hash,
            order=order
        )
        self._N_ref = N_ref
        self._N_ref_args = N_ref_args

    def setDetails(
            self, *,
            N_ref: Callable[..., float] = None, 
            N_ref_args: Tuple[Tuple[Any, ...], Dict[str, Any]] = None,
            order: ComputationalOrder = None
        ):
        # Notes:
        #   * Values will only be modified when their incoming args are not `None`.
        #   * For steps registered using the decorator, all values except for the
        #     `N_ref_args` will be set at the decorator `__init__`.
        #     `N_ref_args` will then be set when the wrapped function is actually called.
        #   * For explicitly-named steps, registered using the context manager, or by calling
        #     `record` directly, all values must be set at the initial call. 
        # 
        if N_ref is not None:
            self._N_ref = N_ref
            self.N_ref_hash = _Step._callable_hash(N_ref)
        if N_ref_args is not None:
            self._N_ref_args = N_ref_args
        if order is not None:
            self.order = order
                 
    @property
    def name(self) -> str:
        # The human-readable name for the step.
        return ".".join(filter(lambda s: bool(s), self.key))

    def N_ref(self) -> float | None:
        # Access the reference value
        N_ref = None
        if self._N_ref is not None:
            N_ref = self._N_ref(**self._N_ref_kwargs)
            """
                if self.mantidSnapper.mtd.doesExist(self.referenceWorkspace):
                    N_ref = float(self.mantidSnapper.mtd[self.referenceWorkspace].getMemorySize())
            elif self.referencePath is not None:
                if self.referencePath.exists():
                    N_ref = float(self.referencePath.stat().st_size)
            """
        return N_ref

    @staticmethod
    def _callable_hash(func: FunctionType) -> str:
        # IMPORTANT: generate a hex digest based on the callable's code, rather than its id!
        #   This then can be used to determine if the `N_ref` callable has changed, between
        # different instances of the persistent `_Step` data.
        if not isinstance(func, FunctionType):
            raise RuntimeError(f"Usage error: expecting `FunctionType` not {type(func)}.")
        sha = sha256()
        sha.update(func.__code__.co_code)
        return sha.hexdigest()
        
    
class _Measurement(BaseModel):
    # Contains post-execution timing data from a specific workflow step.
    
    # elapsed wall-clock time in seconds
    dt: float
    
    # Non-normalized reference value:
    #   as the data point itself: only the original value should be saved.
    # In the case that this value is `None`, this `_Measurement` only includes timing data.
    N_ref: float | None


class _Estimate(BaseModel):
    # A tuple summarizing the information used during the last call to `update`.
    # (<Number of distinct contributing `_Measurement` records>,
    #    <Total number of contributing records>)
    count: Tuple[int, int] | None = None

    # Spline-fit args from most recent "update" calculation.
    #   Possible spline orders can be anything from constant (k == 0),
    #   up to a maximum order  of `Config["workflows_data.timing.spline_order"]`,
    #   which is typically cubic (k == 3).  The order used by the fit is determined
    #   by the number of distinct measurement points available.
    tck: Tuple[List[float], List[float], int] = None

    # A singleton of the default `_Estimate` instance.
    _default: ClassVar[Self] = None
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.tck is not None:
            # Note that this _does_ allow degree `k == 0`: 
            #   but any construction of the actual `BSpline` not using `construct_fast` will
            #   prohibit that.  (See comments at `_update` below.)
            self._spl = BSpline.construct_fast(*self.tck, extrapolate=True)

    @classproperty
    def SPLINE_ORDER(cls):
        return Config["workflows_data.timing.spline_order"]
    
    # To be used during pydantic initialization of other `BaseModel`-derived objects,
    #   for some reason `@classproperty` did not work with the following.
    @classmethod
    def default(cls) -> Self:
        # Due to optimization, and the fact that this method may eventually depend on arguments.
        # default args should not be used to implement this value.
        
        # Implementation notes:
        # * This caches the `_default` value of `_Estimate` as a class attribute.
        # * TODO: an attempt at creating a `cached_classproperty` implementation had some 
        #   issues that need to be resolved.  For the moment, the caching is done explicitly.
        
        if cls._default is None:
            # A linear estimator:
            #   based on 60.0s to process 1GB of data.
            Ns = [0.0, 1.0e9]
            dts = [0.0, 60.0]
            cls._default = _Estimate()
            cls._default._update(Ns, dts, len(Ns))
        
        # IMPORTANT: we need to return a _copy_ here.
        #   To ensure that the `__init__` is called `model_copy` cannot be used.
        return _Estimate(
            count=cls._default.count,
            tck=cls._default.tck
        )
                    
    # Estimate elapsed wall-clock time for a specific workflow step.
    def dt(self, N: float) -> float:
        # Estimate elapsed wall-clock time `dt` for a specific workflow step.
        # `dt` is estimated from normalized 'N'.
        if self._spl is None:
            raise RuntimeError("Usage error: `dt` called before `update`.")
        return float(self._spl(N))

    def update(self, measurements: List[_Measurement], order: Callable[[float], float]):
        # Update the spline model of the time dependence.
        Ns, dts = self._prepareData(measurements, order)
        self._update(Ns, dts, len(measurements))
        
    def _update(self, Ns: np.ndarray, dts: np.ndarray, dataCount: int):
        # Until there are enough distinct measurement points to generate the full spline order,
        #   construct lower-order splines.
        splineOrder = min(len(Ns) - 1, self.SPLINE_ORDER)
        if splineOrder >= 0:
            # retain counts contributing to the estimate: (|<distinct points>|, |<all points>|)
            self.count = (len(Ns), dataCount)
            
            # Compute an interpolating `BSpline` instance (i.e with `s=0.0`).
            # Implementation notes:
            #   * TODO: Optionally, the smoothing parameter 's' can be changed from 0.0.; but that wasn't
            #     explored during this first-pass implementation;
            #   * This treatment also works for degree `k==0` (i.e. a spline representing a _constant_ value),
            #     however constructing a `BSpline` from the resulting `tck` requires the use of `BSpline.construct_fast(*tck)`,
            #     otherwise the knot vector will be judged to be too short.
            self._spl = make_splrep(Ns if not Ns[0] is None else [0.0], dts, k=splineOrder)
            self.tck = (list(self._spl.tck[0]), list(self._spl.tck[1]), self._spl.tck[2])

    def _prepareData(self, measurements: List[_Measurement], order: Callable[[float], float]) -> Tuple[List[float], List[float]]:        
        # Sort and accumulate the measurement data.
        
        ts = [(order(measurement.N_ref) if measurement.N_ref is not None else None, measurement.dt)\
                 for measurement in self.measurements]
        # Either all of the `N_ref` will be `float`, or all will be `None`.
        if ts[0][0] is not None:
            ts = sorted(ts, key=lambda t: t[0])
            if ts[0][0] != 0.0:
                # Insert a "tie" point at (N==0.0, dt==0.0).
                ts.insert(0, (0.0, 0.0))
        Ns, dts = unzip(self._accumulateDuplicates(ts))
        return Ns, dts
            
    def _accumulateDuplicates(self, ps: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        # Accumulate adjacent measurements with the same N values;
        #   as part of the fitting process, this is applied _after_ the computational-order normalization.
        ps_ = []
        p_prev = None
        count = 0
        for p in ps:
            n, dt = p
            if p_prev is not None:
                n_prev, dt_prev = p_prev
                if (n is None and n_prev is None) or np.isclose(n, n_prev):
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

    
class ProgressStep(BaseModel):
    # Information about how to calculate the execution-time estimate
    details: _Step
    
    # Post-execution elapsed wall-clock time measurements for this step.
    measurements: List[_Measurement] = []
    
    # The time estimator for the step.
    estimate: _Estimate = _Estimate.default()
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._startTime: datetime = None
        self._loggingEnabled = False
        
    def setDetails(
        self, *,
        N_ref: Callable[..., Any] = None,
        N_ref_args: Tuple[Tuple[Any, ...], Dict[str, Any]] = None,
        order: ComputationalOrder = None,
        enableLogging: bool = False
        ):
        self.details.setDetails(
                         N_ref=N_ref,
                         N_ref_args=N_ref_args,
                         order=order
                     )           
        self._loggingEnabled = enableLogging
            
    def start(self):
        self._startTime = datetime.now(timezone.utc)
    
    def stop(self) -> datetime:
        time_ = self._startTime
        if time_ is None:
            raise RuntimeError("Usage error: attempt to `stop` unstarted step '{self.details.name}'.")
        self._startTime = None
        return time_
        
    @property
    def startTime(self) -> datetime:
        if self._startTime is None:
            raise RuntimeError("Usage error: attempt to read `startTime` for unstarted step '{self.details.name}'.")
        return self._startTime

    @property
    def loggingEnabled(self) -> bool:
        return self._loggingEnabled
    
class _ProgressRecorder(BaseModel):

    # Map from <step name>s to progress steps.
    #   * A fully-scoped step name is
    #     "<workflow name>.[<workflow name>.,..][<recipe name>.[<recipe name>.,..]]<step name>_<call count>".
    #   * Both the workflow[s] scope and the recipe[s] scope may be nested,
    #     although this feature is not presently used.
    #   * Additional information is associated with the order of the steps in the list.  However
    #     this is also not presently used.
    _stepss: Dict[Tuple[str | None, ...], List[ProgressStep]] = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        """        
        # If elapsed wall-clock time is being recorded:
        #   the workflow stack will always have at least one entry, which is the name of the current workflow.
        self._workflowStack: List[str] = []
        
        # The recipe stack may also have entries, unless the current step is recording
        #   elapsed time within the outer workflow scope.
        self._recipeStack : List[str] = []
        
        # Call counts within the global scope (that of the fully-scoped step name):
        #   this dict is filled in as execution progresses.
        #   If any fully-scoped <step name> is entered more than once,
        #   this allows the additional entries to be distinct.
        self._callCounts: defaultdict[str, int] = defaultdict(int)
        """
        
        # Automatically persist the progress data at application exit.
        atexit.register(_ProgressRecorder._unloadResident)

    @classmethod
    @functools.lru_cache(maxsize=None)
    def instance(cls) -> Self:
        return cls.model_validate_json(LocalDataService().readProgressRecords())

    @classmethod
    def _unloadResident(cls):
        # Unload method to register with `atexit`.
        LocalDataService().writeProgressRecords(cls.instance().model_dump_json())      
        
    """
    def _currentStepName(self) -> str:
        # The fully-scoped <step name> of the current progress-recording step.
        # (Note: <call count> is not dealt with here.)
        if not bool(self._workflowStack):
            raise RuntimeError("Usage error: cannot record progress outside of any workflow.")
        name = ".".join(self._workflowStack)
        if bool(self._recipeStack):
            name = name + "." + ".".join(self._recipeStack)
        return name
    
    # Recording methods using the scope[s] stacks:
    def enterWorkflow(self, servicePath: str):
        self._workflowStack.append(servicePath)
        self.record("entry")
        
    def exitWorkflow(self):
        # End recording for the _current_ (i.e. innermost) workflow.
        self.stop()
        self._workflowStack.pop()
        
    def enterRecipe(self, recipe: Recipe):
        self._recipeStack.append(recipe.__class__.__name__)
        self.record("entry")
    
    def exitRecipe(self):
        # End recording for the _current_ (i.e. innermost) recipe.
        self.stop()
        self._recipeStack.pop()

    def _start(self) -> datetime:
        self._startNextStep()
        self._workflowStartTime = self._stepStartTime
        return self._workflowStartTime
    
    def _end(self):
        pass

    def _startNextStep(self) -> datetime:
        self._stepStartTime = datetime.now().()
        return self._stepStartTime
               
    def register(self, workflowName: str, steps: List[Step]):
        # Register a list of progress-recording steps, 
        #   and where possible, do not overwrite existing information.
        
        == workflowName.methodName ==
        workflow_steps = self._stepss.get(workflowName, {})
        # Add the default "start" and "end" steps.
        if "start" not in workflow_steps:
            workflow_steps["start"] = ProgressStep(
                details=Step(name="start"),
                measurements=[],
                estimate=_Estimate.default()
            )
        if "end" not in workflow_steps:
            workflow_steps["end"] = ProgressStep(
                details=Step(name="end"),
                measurements=[],
                estimate=_Estimate.default()
            )
       
        for step in steps:
            _step = workflow_steps.get(step.name)
            if bool(_step) and _step == step:
                # Step details haven't changed:
                #   use the existing execution-time data.
                continue
            
            # Not a previously registered step:
            #   initialize a new step.
            workflow_steps[step.name] = ProgressStep(
                details=step,
                measurements=[],
                estimate=_Estimate.default()
            )

        == DID we just lose the "ordering" here? ==
        self._stepss[workflowName] = workflow_steps
    """

    @classmethod
    def _getCallerFullyQualifiedName(cls, callerObjectOrStackFrame) -> Tuple[str | None, ...]:
        # Caller key based on its (<module>, <fully-qualified name>).
        # args:
        #   'callerObjectOrStackFrame': this will be either a suitable unique reference for the caller itself
        #      as a `class`, `<class instance>`, `method`, or `function,
        #      or it will be the stack frame to look at to obtain the caller's information.
        # Note that the only requirement here is that the information determined about the caller be
        # suitably unique, when combined with any [optional] explicit <step name>
        
        module_name = None
        qualified_name = None
        if not isinstance(callerObjectOrStackFrame, FrameType):
            caller = callerObjectOrStackFrame
            module_name = caller.__module__
            if isinstance(caller, FunctionType):
                qualified_name = caller.__qualname__
            elif isinstance(caller, type):
                qualified_name = caller.__class__.__qualname__
            else:
                raise RuntimeError(
                    f"Not implemented: fully-qualified name for object of type {type(caller)}.\n"
                    + "  Expecting `type` or `FunctionType` only."            
                )       
        else:
            frame = callerObjectOrStackFrame
            module = inspect.getmodule(frame.f_code)
            module_name = module.__name__ if module is not None else ""
            if sys.version_info >= (3, 11, 0):
                qualified_name = frame.f_code.co_qualname
            else:
                # Required if Python version < 3.11.

                # Get the function name
                func_name = frame.f_code.co_name

                # If the function is a method within a class, try to find the class name
                class_name = None
                if 'self' in frame.f_locals:
                    class_name = frame.f_locals['self'].__class__.__qualname__
                elif 'cls' in frame.f_locals:
                    class_name = frame.f_locals['cls'].__qualname__

        return (module_name, qualified_name)

    @classmethod
    def isLoggingEnabledForStep(cls, caller):
        if not isinstance(caller, FunctionType):
            raise RuntimeError(f"Usage error: expecting `FunctionType` not {type(caller)}.")
        if issubclass(caller.__class__, Service)\
            and Config["workflows_data.timing.service_logging"]:
            return True
        if issubclass(caller.__class__, (GenericRecipe, Recipe))\
            and Config["workflows_data.timing.recipe_logging"]:
            return True
        # By default, all progress logging must be explicitly enabled.
        return False
        
    @classmethod
    def getStepKey(cls, *, callerOverride: MethodType | FunctionType=None, stepName=None) -> Tuple[str | None, ...]:
        # Compute a unique key for the current process step.
        
        # Notes:
        #   * In general, the key tuple will be `(<module name>, <fully qualified class or method name>, <explicit step name>)`.
        #   * `None` is a valid entry for any tuple component;
        #   * Caller's frame is actually two-frames up from the current frame.
        
        if bool(callerOverride) and not isinstance(callerOverride, (MethodType, FunctionType)):
            # Profiling of class types is supported,
            #   but the target-method to be profiled should have been filled in prior to calling this method.
            raise RuntimeError("Usage error: when `callerOverride` is set, it must either be a method or a function.")            
        
        callerObjectOrStackFrame = callerOverride if callerOverride is not None else inspect.currentframe().f_back.f_back
        key = cls._getCallerFullyQualifiedName(callerObjectOrStackFrame) + (stepName,)
        return key

    def getStep(
            self,
            key: Tuple[str | None, ...],
            create: bool = False
        ) -> ProgressStep:
        # Get the `ProgressStep` corresponding to the specified step key.
        step = self._stepss.get(key)
        if step is None:
            if not create:
                raise RuntimeError(f"Usage error: progress-recording step '{key}' does not exist.")
            step = ProgressStep(
                       details=_Step(
                           key=key
                       )
                   )
        return step

                
    def record(
            self,
            *,
            callerOverride: FunctionType = None, 
            stepName: str = None,
            N_ref: Callable[..., float] = None,
            N_ref_args: Tuple[Tuple[Any, ...], Dict[str, Any]] = None,
            order: ComputationalOrder = None,
            enableLogging=False
        ) -> Tuple[str | None, ...]:
        # Initialize the elapsed wall-clock time measurement for a processing step.
        # args: 
        #   'stepName': [optional] if provided this is an additional suffix key added to
        #      the step key generated from the _local_ (e.g. <class instance>, <method>, <function>, or <module>) scope.
        #   'N_ref': an optional callable
        #   'N_ref_args': the args to be used when calling `N_ref` as `(args, kwargs)`
        #   'order': the computational order of the step.  This will be used to _normalize_ the `N_ref` value prior to estimating the step.
        # returns:
        #   the tuple of str keys used to uniquely identify the step
        
        # Usage notes:
        #   * Each call to `record` _must_ have a corresponding call to `stop`
        #     -- these calls are matched using the step key.
        #     This requirement will generally be enforced using the context manager, and / or its decorator
        #     as implemented below.  Alternatively, this requirement can be enforced 
        #     by calling `stop` within a `finally` block.
        #   * Other than the previous, calls to `record` can be nested as required by the process being profiled.
        #   * IMPORTANT: any call to `stop` should only record the measurement if no exception has been raised.
        #     This is quite important as a mis-recorded measurement may affect all future estimates!
        #   * The `N_ref` callable will in general accept the same arguments as the function whose progress is being recorded.
        #     Alternatively, for explicitly named sub-steps, any `N_ref` that matches the specified `N_ref_args` will be fine.
        #   * IMPORTANT: `N_ref` should return `None` if no value can be calculated.  This ensures that no persistent
        #     measurement data will be entered based on such a value.  For example, in `ReductionService`, the time estimate
        #     for the live-data case is not yet implemented.  When the `N_ref` sees that the `request.liveData` flag is set,
        #     it returns `None`.
        #   * When an `N_ref` is not specified, the default will be to treat the estimate for the process step
        #     as scaling with _constant_ time -- i.e the estimate will be average of past execution times for the step.
        
        
        key = self.getStepKey(callerOverride=callerOverride, stepName=stepName)
        step = self.getStep(key, create=True)
        step.setDetails(N_ref=N_ref, N_ref_args=N_ref_args, order=order)
        step.start()
        
        if step.loggingEnabled:
            N_ref_ = step.details.N_ref()
            if N_ref_ is not None:
                dt = step.estimate.dt(step.details.order(N_ref))
                logger.log(Config["workflows_data.timing.loglevel"], f"running {step.name} -- estimated completion in {dt} seconds.")
            else:
                logger.log(Config["workflows_data.timing.loglevel"], f"running {step.name} -- <no timing data is available>.")
        """
        # Get the elapsed time, and start timing the next step.
        startStep = self._startStepTime
        elapsed = float((self._startNextStep() - startStep).total_seconds())

        # FINALIZE the measurement data for the previous step.
        if self.currentStep is not None:
            # Compute the required reference value.
            == `N_ref()` calls the `Callable` passed in when the step was registered,
            ==   using the *step_args passed into `record`! ==
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
            estimated_dt = self.currentStep.estimate.dt(self.currentStep.details.order(N_ref) if N_ref is not None else None)
            if abs(estimated_dt - elapsed) / elapsed > Config["workflows_data.timing.update_threshold"]:
                self.currentStep.estimate.update(self.currentStep.measurements, self.currentStep.details.order)
                
        # Go to the next step.
        self.currentStep = step
        """
        
    def stop(self, key: Tuple[str | None, ...]):
        step = self.getStep(key)
        now = datetime.now(timezone.utc)
        startTime = step.stop()
        
        # Do not record the measurement if any exception has been raised.
        if sys.exc_info[0] is not None:
            elapsed = float((now - startTime).total_seconds())
            N_ref = step.details.N_ref()
            # Do not record the measurement if `N_ref` cannot be calculated.
            if N_ref is not None:
                step.measurements.append(
                    _Measurement(dt=elapsed, N_ref=N_ref) 
                ) 
        
                # Restrict the maximum length of the measurements list.
                max_measurements = Config["workflows_data.timing.max_measurments"]
                if len(step.measurements) > max_measurements:
                    # Forget the oldest
                    self.currentStep.measurements = self.currentStep.measurements[-max_measurements:]

                # If necessary, update the estimate for the step.
                estimated_dt = step.estimate.dt(step.details.order(N_ref) if N_ref is not None else None)
                if abs(estimated_dt - elapsed) / elapsed > Config["workflows_data.timing.update_threshold"]:
                    step.estimate.update(step.measurements, step.details.order)
            
    def stepTimeRemaining(self, key: Tuple[str | None, ...]) -> float:
        # Estimate the time remaining for the specified step,
        # or return `None` if not enough data is available yet to create an estimate,
        step = self.getStep(key)

        elapsed = float((step.startTime - datetime.now(timezone.utc)).total_seconds())        
        N_ref = step.details.N_ref()
        remainder = None
        if N_ref is not None:
            remainder = step.estimate.dt(step.details.order(N_ref)) - elapsed
            if remainder < 0.0:
                remainder = 0.0
        return remainder
    
    """
    def workflowTimeRemaining(self, workflowInstance) -> float:
        # Estimate the time remaining for the current workflow.
        # (This will only be accurate for the case where all reference-workspaces or paths
        #  are available at the time of this call!)

        # Validate the workflow registration.
        workflowName = workflowInstance.__class__.__name__
        if workflowName not in self._stepss:
            raise RuntimeError(f"Unregistered workflow: '{workflowName}'.")
        steps = self._stepss[workflowName]
        
        default_N_ref = 1.0e9 # use when actual N_ref for a step doesn't exist yet
        now = datetime.now().astimezone()
        start = self._workflowStartTime if bool(self._workflowStartTime) else now
        elapsed = float((now - start).total_seconds())
        
        total = 0.0
        for step in steps: == ordered dict? ==
            N_ref = step.N_ref() == this only works if the *args are something simple, like `request` ==
            if N_ref is None:
                N_ref = default_N_ref
            total += step.estimate.dt(step.details.order(N_ref)) == that is, maybe we don't have all to compute `N_ref` yet! ==
        
        remainder = total - elapsed
        if remainder < 0.0:
            remainder = 0.0
        return remainder 
    """

# The singleton of `_ProgressRecorder`.
ProgressRecorder = _ProgressRecorder.instance()


class WallClockTime():
    # A decorator or context manager to register a process-recording step for any method, function, or class.
        
    def __init__(
            self, *,
            callerOverride: str = None, 
            stepName: str = None,
            N_ref: Callable[..., float] = None,
            N_ref_args: Tuple[Tuple[Any, ...], Dict[str, Any]] = None,
            order: ComputationalOrder = None,
            enableLogging: bool = False
        ):
        # When used as a decorator:
        #   -- `callerOverride` is defined _only_ if the decoratee is a class:
        #      in this case it specifies the name of the class method to be profiled;
        #   -- `stepName` must not be defined;
        #   -- `N_ref` may optionally be defined;
        #   -- `N_ref_args` must not be defined;
        #   -- `order` may optionally be defined.
        # When used as a context manager:
        #   -- `callerOverride` should not be defined;
        #   -- `stepName` must be defined;
        #   -- `N_ref` and `N_ref_args` may optionally be defined;
        #   -- `order` may optionally be defined.
        # In either case:
        #   -- when `N_ref`, `N_ref_args`, and `order` are left as `None`:
        #      at time of execution, a constant-time estimate will be used.

        self.callerOverride = callerOverride
        self.stepName = stepName
        self.N_ref = N_ref
        self.N_ref_args = N_ref_args
        self.order = order
        self._stepKey = None
        self._enableLogging = enableLogging
        
    def __call__(self, decoratee: type | FunctionType ) -> type | FunctionType:
        # Apply as a decorator
        
        if bool(self.stepName):
            raise RuntimeError(
                      "Usage error: on a `WallClockTime` decorated function or class:\n"
                      + "  `stepName` must not be specified."
                  )
                  
        func = decoratee
        if isinstance(decoratee, type):
            if not bool(self.callerOverride):
                raise RuntimeError(
                          "Usage error: on a `WallClockTime` decorated class:\n"
                          + "  `callerOverride` must be specified: this is the name of the to-be-profiled method.\n"
                      )
            func = decoratee.__getattribute__(callerOverride)
        
        @functools.wraps(func)  # Retain metadata about `func`.
        def _wrapper(*args, **kwargs):
            # Allow the called function's `args`, and `kwargs` to be available
            #   to the `N_ref` used by the progress-reporting step.
            try:
                if self._stepKey is not None:
                    raise RuntimeError(
                              "Usage error: `WallClockTime` as decorator cannot be nested or re-used."
                          )
                self._stepKey = ProgressRecorder.record(
                    callerOverride=func,
                    N_ref=self.N_ref,
                    N_ref_args=(args, kwargs),
                    order=self.order,
                    enableLogging=self._enableLogging or _ProgressRecorder.isLoggingEnabledForStep(func)
                )
                result = func(*args, **kwargs)
            finally:
                if self._stepKey is not None:
                    ProgressRecorder.stop(self._stepKey)
            return result

        if isinstance(decoratee, type):
            decoratee.__setattribute__(self.callerOverride, _wrapper)
            return decoratee
        return _wrapper

    def __enter__(self):        
        if bool(self.callerOverride) or not bool(self.stepName):
            raise RuntimeError(
                      "Usage error: `WallClockTime` as context manager:\n"
                      + "  `callerOverride` must not be specified;\n"
                      + "  `stepName` must be specified."
                  )
        if self._stepKey is not None:
            raise RuntimeError(
                      "Usage error: `WallClockTime` as context manager cannot be nested or re-used."
                  )
        self._stepKey = ProgressRecorder.record(
            stepName=self.stepName,
            N_ref=self.N_ref,
            N_ref_args=self.N_ref_args,
            order=self.order,
            enableLogging=self._enableLogging
        )
    
    def __exit__(self, *exc):
        if self._stepKey is not None:
            ProgressRecorder.stop(self._stepKey)
        if bool(exc[0]):
            raise
