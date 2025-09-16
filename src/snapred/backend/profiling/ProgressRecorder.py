import atexit
import functools
import inspect
import sys
from datetime import datetime, timezone
from enum import StrEnum
from hashlib import sha256
from threading import Timer
from types import FrameType, FunctionType, MethodType
from typing import Any, Callable, ClassVar, Dict, List, Self, Tuple

import numpy as np
from pydantic import BaseModel, field_serializer, field_validator
from scipy.interpolate import BSpline, make_splrep

from snapred.backend.api.PubSubManager import PubSubManager
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty

logger = snapredLogger.getLogger(__name__)


class ComputationalOrder(StrEnum):
    O_0 = "O_0"
    O_LOG_N = "O_LOG_N"
    O_N = "O_N"
    O_N_LOG_N = "O_N_LOG_N"
    O_N_2 = "O_N_2"
    O_N_3 = "O_N_3"

    def __call__(self, N_ref: float) -> float:
        # Incoming 'N_ref' must always have a value:
        #   even in the case of `O_0`.
        if N_ref is None:
            raise RuntimeError("Usage error: incoming 'N_ref' must have a value.")
        N = None
        match self:
            case ComputationalOrder.O_0:
                N = 1.0
            case ComputationalOrder.O_LOG_N:
                N = np.log(N_ref)
            case ComputationalOrder.O_N:
                N = N_ref
            case ComputationalOrder.O_N_LOG_N:
                N = N_ref * np.log(N_ref)
            case ComputationalOrder.O_N_2:
                N = N_ref ** (2.0)
            case ComputationalOrder.O_N_3:
                N = N_ref ** (3.0)
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
    N_ref_hash: str | None

    # Standard computational-order is used to normalize `N_ref`, from the as-saved measurement,
    #   to `N`, as used by the estimate's spline fit.
    order: ComputationalOrder | None

    ##
    ## PRIVATE attributes:
    ##

    # `_N_ref`, `_N_ref_args`:
    # The actual `N_ref` function corresponding to this process step, and its `(args, kwargs)` tuple.
    #   `N_ref` computes the scalar value to be used as input to the computational-order calculation:
    #   `dt = <order>(N_ref(*args, **kwargs))`, where `<order>` is a standard process order, e.g. `N_log_N`,
    #   and `dt` is the estimated execution time.

    def __init__(
        self,
        key: Tuple[str | None, ...],
        *,
        N_ref_hash: str | None = None,
        N_ref: Callable[..., float] | None = None,
        N_ref_args: Tuple[Tuple[Any, ...], Dict[str, Any]] = ((), {}),
        order: ComputationalOrder = None,
    ):
        # The persistent part of the `Step` retains the hash digest of the `N_ref`-callable,
        #   but not the callable itself.

        # In expected usage: during `__init__` only ONE of these would be specified.
        if N_ref is not None:
            if N_ref_hash is not None:
                raise RuntimeError(
                    f"Usage error: step {key} initialization should specify "
                    + "either `N_ref` or `N_ref_hash`, but not both."
                )
            N_ref_hash = _Step._callable_hash(N_ref)

        super().__init__(key=key, N_ref_hash=N_ref_hash, order=order)
        self._N_ref = N_ref
        self._N_ref_args = N_ref_args

    # A factory method to produce default instances of `_Step`.
    @classmethod
    def default(cls, key: Tuple[str, ...]) -> Self:
        return _Step(
            key=key
            # Default values for `N_ref` and `order` are set
            #   at `WallClockTime`.  They should not be set here.
        )

    def setDetails(
        self,
        *,
        N_ref: Callable[..., float] = None,
        N_ref_args: Tuple[Tuple[Any, ...], Dict[str, Any]] = None,
        order: ComputationalOrder = None,
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
            previousHash = self.N_ref_hash
            self.N_ref_hash = _Step._callable_hash(N_ref)
            if previousHash is not None and self.N_ref_hash != previousHash:
                logger.warning(
                    f"`N_ref` for step {self.key} has been modified from that used in previously-saved data.\n"
                    + "  In this case, usually the previous timing measurements for the step should be discarded."
                )

        if N_ref_args is not None:
            self._N_ref_args = N_ref_args
        if order is not None:
            self.order = order

    def N_ref(self) -> float | None:
        # Access the reference value
        N_ref = None
        if self._N_ref is not None:
            N_ref = self._N_ref(*self._N_ref_args[0], **self._N_ref_args[1])
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
        return sha.hexdigest()[0:16]


class _Measurement(BaseModel):
    # Contains post-execution timing data from a specific workflow step.

    # elapsed wall-clock time in seconds
    dt: float

    # estimate of elapsed wall-clock time
    dt_est: float | None

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
    #   up to a maximum order  of `Config["application.workflows_data.timing.spline_order"]`,
    #   which is typically cubic (k == 3).  The order used by the fit is determined
    #   by the number of distinct measurement points available.
    tck: Tuple[List[float], List[float], int] = None

    # A singleton of the default `_Estimate` instance.
    _default: ClassVar[Self] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._spl = None
        if self.tck is not None:
            # Note that this _does_ allow degree `k == 0`:
            #   but any construction of the actual `BSpline` not using `construct_fast` will
            #   prohibit that.  (See comments at `_update` below.)
            _tck = (np.array(self.tck[0]), np.array(self.tck[1]), self.tck[2])
            self._spl = BSpline.construct_fast(*_tck, extrapolate=True)

    @classproperty
    def SPLINE_DEGREE(cls):
        return Config["application.workflows_data.timing.spline_order"]

    # A factory method to produce a default instance of the `_Estimate`.
    @classmethod
    def default(cls) -> Self:
        # A factory method to produce default instances of `_Estimate`.

        # Implementation notes:
        # * As this method may eventually depend on arguments,
        #   default args should not be used to implement this value.
        # * This caches the `_default` value of `_Estimate` as a class attribute.

        if cls._default is None:
            # A linear estimator:
            #   based on: 3.0s to process 1GB of data.
            Ns = np.array([0.0, 1.0e9])
            dts = np.array([0.0, 3.0])
            cls._default = _Estimate()
            cls._default._update(Ns, dts, len(Ns))

        # IMPORTANT: we need to return a _copy_ here.
        #   To ensure that the `__init__` is called `model_copy` cannot be used.
        return _Estimate(count=cls._default.count, tck=cls._default.tck)

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
        splineOrder = min(len(Ns) - 1, self.SPLINE_DEGREE)
        if splineOrder >= 0:
            # retain counts contributing to the estimate: (|<distinct points>|, |<all points>|)
            self.count = (len(Ns), dataCount)

            # Compute an interpolating `BSpline` instance (i.e with `s=0.0`).
            # Implementation notes:
            #   * TODO: Optionally, the smoothing parameter 's' can be changed from 0.0.; but that wasn't
            #     explored during this first-pass implementation;
            #   * This treatment also works for degree `k==0` (i.e. a spline representing a _constant_ value),
            #     however constructing a `BSpline` from the resulting `tck` requires the use of
            #     `BSpline.construct_fast(*tck)`, otherwise the knot vector will be judged to be too short.
            self._spl = make_splrep(Ns if Ns[0] is not None else np.array([0.0]), dts, k=splineOrder)
            self.tck = (list(self._spl.tck[0]), list(self._spl.tck[1]), self._spl.tck[2])

    def _prepareData(
        self, measurements: List[_Measurement], order: Callable[[float], float]
    ) -> Tuple[np.ndarray, np.ndarray]:
        # Sort and accumulate the measurement data.

        ts = [
            (order(measurement.N_ref) if measurement.N_ref is not None else None, measurement.dt)
            for measurement in measurements
        ]

        # Either all of the `N_ref` will be `float`, or all will be `None`.
        if any(t[0] is None for t in ts) and not all(t[0] is None for t in ts):
            raise RuntimeError("Either all of the `N_ref` values should be `float`, or all should be `None`.")

        if ts[0][0] is not None:
            ts = sorted(ts, key=lambda t: t[0])
            if ts[0][0] != 0.0:
                # Insert a "tie" point at (N==0.0, dt==0.0).
                ts.insert(0, (0.0, 0.0))
        Ns, dts = self._accumulateDuplicates(ts)
        return np.array(Ns), np.array(dts)

    def _accumulateDuplicates(self, ps: List[Tuple[float, float]]) -> Tuple[List[float], List[float]]:
        Ns_, dts_ = [], []
        i = 0
        while i < len(ps):
            n_group, dt_values = ps[i][0], [ps[i][1]]
            j = i + 1
            while j < len(ps):
                n_curr = ps[j][0]
                if (n_group is None and n_curr is None) or np.isclose(n_group, n_curr):
                    dt_values.append(ps[j][1])
                    j += 1
                else:
                    break
            Ns_.append(n_group)
            dts_.append(np.mean(dt_values))
            i = j
        return Ns_, dts_

    @field_validator("tck", mode="before")
    @classmethod
    def _validate_tck(cls, v: Any) -> Tuple[List[float], List[float], int]:
        if v is not None:
            if v[2] > cls.SPLINE_DEGREE:
                raise ValueError(f"In `tck`, specified spline degree `k` must be <= {_Estimate.SPLINE_DEGREE}.")
        return v


class ProgressStep(BaseModel):
    # Progress-recording information for a single process step.

    # Information about how to calculate the execution-time estimate
    details: _Step

    # Elapsed wall-clock time measurements for previous executions of this step.
    measurements: List[_Measurement] = []

    # An execution-time estimator for this step.
    estimate: _Estimate = _Estimate.default()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._isSubstep = False
        self._startTime: datetime | None = None
        self._N_ref: float | None = None
        self._dt: float | None = None
        self._loggingEnabled = False
        self._timer: Timer | None = None

    def setDetails(
        self,
        *,
        N_ref: Callable[..., Any] = None,
        N_ref_args: Tuple[Tuple[Any, ...], Dict[str, Any]] = None,
        order: ComputationalOrder = None,
        enableLogging: bool = False,
    ):
        self.details.setDetails(N_ref=N_ref, N_ref_args=N_ref_args, order=order)
        self._loggingEnabled = enableLogging

    def start(self, isSubstep: bool):
        self._isSubstep = isSubstep
        self._startTime = datetime.now(timezone.utc)

        # Set the expected execution-time duration:
        #   it is IMPORTANT that this be the only place where this estimate is calculated.
        #   For example, it should not be recalculated prior to logging.
        self._N_ref = self.details.N_ref()
        if self._N_ref is not None:
            self._dt = self.estimate.dt(self.details.order(self._N_ref))
        if not isSubstep:
            PubSubManager().publish("progress", {"step": self.name, "dt_rem": self.dt, "dt_est": self.dt})

    def stop(self):
        if not self.isActive:
            raise RuntimeError(f"Usage error: attempt to `stop` unstarted step {self.name}.")
        self._isSubstep = False
        self._startTime = None
        self._N_ref = None
        self._dt = None
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def recordMeasurement(self, dt_elapsed: float, dt_est: float, N_ref: float):
        # Record a measurement in the `measurements` list.

        # The possibility of active exceptions, or of an `N_ref` value of `None` should have been treated
        #   outside of this method.
        self.measurements.append(_Measurement(dt=dt_elapsed, dt_est=dt_est, N_ref=N_ref))

        # Restrict the maximum length of the measurements list.
        max_measurements = Config["application.workflows_data.timing.max_measurements"]
        if len(self.measurements) > max_measurements:
            # Forget the oldest measurements
            self.measurements = self.measurements[-max_measurements:]

        # If necessary, update the estimate for the step.
        if len(self.measurements) >= Config["application.workflows_data.timing.update_minimum_count"]:
            # Enough data points are available to perform an update.
            if abs(dt_est - dt_elapsed) / dt_elapsed > Config["application.workflows_data.timing.update_threshold"]:
                # The relative error from the current estimate is greater than the threshold.
                self.estimate.update(self.measurements, self.details.order)

    @property
    def name(self) -> str:
        # A human-readable name for the step.
        return self.humanReadableName(self.details.key)

    @classmethod
    def humanReadableName(cls, key: Tuple[str, ...]) -> str:
        return ".".join(filter(lambda s: bool(s), key))

    @classmethod
    def shortName(cls, key: Tuple[str, ...], isSubstep: bool) -> str:
        # Return an abbreviated name for the step, retaining only the strictly necessary components.
        if isSubstep:
            # the substep name
            return key[-1]
        name = key[-1] if bool(key[-1]) else ""
        qualname = key[-2]

        # the <step name> appended to the last component of the `qualname`
        return ": ".join(filter(None, (qualname.split(".")[-1], name)))

    @property
    def isActive(self) -> bool:
        return self._isActive()

    def _isActive(self) -> bool:
        # separate method for testing
        return bool(self._startTime)

    @property
    def startTime(self) -> datetime:
        if self._startTime is None:
            raise RuntimeError("Usage error: attempt to read `startTime` for unstarted step '{self.details.name}'.")
        return self._startTime

    @property
    def N_ref(self) -> float | None:
        # The reference value used to estimate the execution time for an active step:
        #   a value of `None` indicates that an estimate could not be calculated.
        if not self.isActive:
            raise RuntimeError("Usage error: attempt to read `N_ref` for unstarted step '{self.details.name}'.")
        return self._N_ref

    @property
    def dt(self) -> float | None:
        # The estimated execution-time duration for an active step (in seconds):
        #   a value of `None` indicates that an estimate could not be calculated.
        if not self.isActive:
            raise RuntimeError("Usage error: attempt to read `dt` for unstarted step '{self.details.name}'.")
        return self._dt

    @property
    def loggingEnabled(self) -> bool:
        return self._loggingEnabled

    @property
    def isSubstep(self) -> bool:
        return self._isSubstep


class _ProgressRecorder(BaseModel):
    # Map from <step key> to progress steps.
    #   * `Dict[Tuple[str | None, ...], ProgressStep]` is the primary class,
    #        the `List[ProgressStep]` is only used for serialization, to work around the fact
    #        that JSON `dict` require string keys.
    #   * A fully-scoped step key includes
    #       (<module name>, <the fully-qualified function name of the caller>, <step name>)
    #       any element of the tuple may be `None`.  For example, the <step name> is usually `None`
    #       when the step is associated with the `WallClockTime` used as a decorator.
    #   * Both workflow scope and recipe scope may be nested,
    #       and a step can be identified as a substep if there are already steps on the active-step stack
    #       when it begins execution.
    steps: Dict[Tuple[str | None, ...], ProgressStep] | List[ProgressStep] = {}

    def __new__(cls, *_args, **_kwargs):
        # This is declared as a pass-through method, to be used during testing.
        return super().__new__(cls)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._activeSteps: List[ProgressStep] = []

    @classmethod
    @functools.lru_cache(maxsize=None)
    def _loggable_qualname_roots(cls):
        # TODO: PROBLEM -- this bypasses `Config` reload!
        # Convert this `Config` value to a set, so that it won't scanned each time it's used
        return set(Config["application.workflows_data.timing.logging.qualname_roots"])

    @classproperty
    def enabled(cls):
        return Config["application.workflows_data.timing.enabled"]

    @classmethod
    @functools.lru_cache(maxsize=None)
    def instance(cls) -> Self:
        # TODO: PROBLEM -- this bypasses `Config` reload!
        if cls.enabled:
            if Config["application.workflows_data.timing.persistent_data"]:
                # Automatically persist the progress data at application exit.
                atexit.register(_ProgressRecorder._unloadResident)

            return cls.model_validate_json(LocalDataService().readProgressRecords())

        # When not enabled, we still need to return an "empty" instance.
        return cls()

    @classmethod
    def _unloadResident(cls):
        # Unload method to register with `atexit`.
        try:
            if cls.enabled:
                LocalDataService().writeProgressRecords(cls.instance().model_dump_json(indent=2))
        except BaseException:  # noqa: BLE001
            # This method is registered with `atexit`: it must not raise any exceptions.
            pass

    @classmethod
    def _getCallerFullyQualifiedName(cls, callerObjectOrStackFrame) -> Tuple[str | None, ...]:
        # Caller key based on its (<module>, <fully-qualified name>).
        # args:
        #   'callerObjectOrStackFrame': this will be either a reference to the caller itself
        #      as a `class`, `<class instance>`, `method`, or `function,
        #      OR it will be the stack frame to look at to examine and obtain the caller's information.
        # Note that the only requirement here for progress-step scoping,
        # is that the information determined about the caller be
        # suitably unique, when combined with the [optional] explicit <step name>.

        module_name = None
        qualified_name = None
        if not isinstance(callerObjectOrStackFrame, FrameType):
            caller = callerObjectOrStackFrame
            if isinstance(caller, FunctionType):
                qualified_name = caller.__qualname__
            elif isinstance(caller, type):
                qualified_name = caller.__qualname__
            else:
                raise RuntimeError(
                    f"Not implemented: fully-qualified name for object of type {type(caller)}.\n"
                    + "  Expecting object of type `FrameType`, `FunctionType`, or `type` only."
                )
            module_name = caller.__module__
        else:
            frame = callerObjectOrStackFrame
            module = inspect.getmodule(frame.f_code)
            module_name = module.__name__ if module is not None else ""

            qualified_name = None
            if sys.version_info >= (3, 11, 0):
                qualified_name = frame.f_code.co_qualname
            else:
                # Required if Python version < 3.11.

                # Get the function name
                func_name = frame.f_code.co_name

                # If the function is a method within a class, try to find the class name
                class_name = None
                if "self" in frame.f_locals:
                    class_name = frame.f_locals["self"].__class__.__qualname__
                elif "cls" in frame.f_locals:
                    class_name = frame.f_locals["cls"].__qualname__

                qualified_name = class_name + "." + func_name if bool(class_name) else func_name

        return (module_name, qualified_name)

    @classmethod
    def isLoggingEnabledForStep(cls, key: Tuple[str, ...]) -> bool:
        # Determine whether or not to log a step, based on the qualified name of its scope.

        # key components: (<module name>, <fully-qualified scope name>, <explicit step name>)
        qualname = key[-2]
        if qualname is not None:
            _root = qualname.split(".")[0]
            return _root in cls._loggable_qualname_roots()
        return False

    @classmethod
    def getStepKey(
        cls, *, callerOrStackFrameOverride: FunctionType | FrameType | type = None, stepName=None
    ) -> Tuple[str | None, ...]:
        # Compute a unique key for the current process step.

        # Notes:
        #   * In general, the key tuple will be
        #     `(<module name>, <fully qualified class or method name>, <explicit step name>)`.
        #   * `None` is a valid entry for any tuple component;
        #   * Note that the caller's stack frame is actually two-frames up from the current frame.

        if bool(callerOrStackFrameOverride) and not isinstance(
            callerOrStackFrameOverride, (FunctionType, FrameType, type)
        ):
            raise RuntimeError(
                "Usage error: when `callerOrStackFrameOverride` is set, it must be either "
                + "a function, the local stack-frame of a function, or a <class> type."
            )

        # The choice of stack frame in the fallback here allows `ProgressRecorder.record` to be called directly
        # in the scope of a function of interest.  Any other usage should supply the <caller function>
        # or <local stack-frame of caller function> directly.
        # TODO: maybe there shouldn't be any fallback?
        callerObjectOrStackFrame = (
            callerOrStackFrameOverride
            if callerOrStackFrameOverride is not None
            else inspect.currentframe().f_back.f_back
        )
        key = cls._getCallerFullyQualifiedName(callerObjectOrStackFrame) + (stepName,)
        return key

    def getStep(self, key: Tuple[str | None, ...], create: bool = False) -> ProgressStep:
        # Get the `ProgressStep` corresponding to the specified step key.
        step = self.steps.get(key)
        if step is None:
            if not create:
                raise RuntimeError(f"Usage error: progress-recording step {key} does not exist.")
            step = ProgressStep(details=_Step(key=key))
            self.steps[key] = step
        return step

    def record(
        self,
        *,
        callerOrStackFrameOverride: MethodType | FunctionType | FrameType = None,
        stepName: str = None,
        N_ref: Callable[..., float] = None,
        N_ref_args: Tuple[Tuple[Any, ...], Dict[str, Any]] = None,
        order: ComputationalOrder = None,
        enableLogging=False,
    ) -> Tuple[str | None, ...]:
        # Initialize the elapsed wall-clock time measurement for a processing step.
        # args:
        #   'stepName': [optional] if provided this is an additional suffix key added to
        #      the step key generated from the _local_ (e.g. <class instance>, <method>, <function>, or <module>) scope.
        #   'N_ref': an optional callable
        #   'N_ref_args': the args to be used when calling `N_ref` as `(args, kwargs)`
        #   'order': the computational order of the step.
        #     This will be used to _normalize_ the `N_ref` value prior to estimating the step.
        # returns:
        #   the tuple of str keys used to uniquely identify the step

        # Usage notes:
        # -- Each call to `record` _must_ have a corresponding call to `stop`:
        #    these calls are matched using the step key.
        # -- Calls to `record` may be nested as required by the process being profiled.
        # -- IMPORTANT: any call to `stop` should only record the measurement if no exception has been raised.
        #    Mis-recorded measurements affect future estimates!
        # -- There are several important details relating to the definition and behavior of the `N_ref`
        #    function.  Please see:
        #    `snapred.readthedocs.io/en/latest/developer/implementation_notes/profiling_and_progress_recording.html`.

        if not _ProgressRecorder.enabled:
            return None

        key = self.getStepKey(callerOrStackFrameOverride=callerOrStackFrameOverride, stepName=stepName)
        step = self.getStep(key, create=True)

        # Logging is either enabled by explicit request,
        #   or is enabled depending on the specifics of the scope of the step's key.
        enableLogging |= ProgressRecorder.isLoggingEnabledForStep(key)

        step.setDetails(N_ref=N_ref, N_ref_args=N_ref_args, order=order, enableLogging=enableLogging)
        step.start(isSubstep=len(self._activeSteps) > 0)

        # Push to the active steps stack.
        self._activeSteps.append(step)

        if step.loggingEnabled:
            self._chainLogTimeRemaining(step)

        # Return the key
        return key

    def stop(self, key: Tuple[str | None, ...]):
        if not _ProgressRecorder.enabled:
            return

        step = self.getStep(key)

        # retain timing-property values while the step is still active
        startTime = step.startTime
        N_ref = step.N_ref
        dt = step.dt
        isSubstep = step.isSubstep

        # all cached step properties are cleared beyond this point
        step.stop()

        # Pop from the active steps stack.
        if not bool(self._activeSteps):
            raise RuntimeError("Usage error: `_activeSteps` stack underflow.")
        self._activeSteps.pop()

        # Do not output anything to the log, or record the measurement,
        #   if any exception has been raised.
        if sys.exc_info()[0] is None:
            if step.loggingEnabled:
                self._logCompletion(step.details.key, isSubstep)

            stopTime = datetime.now(timezone.utc)
            elapsed = float((stopTime - startTime).total_seconds())

            # Do not record the measurement if `N_ref` could not be calculated.
            if N_ref is not None:
                step.recordMeasurement(dt_elapsed=elapsed, dt_est=dt, N_ref=N_ref)

    def logTimeRemaining(self, key: Tuple[str | None, ...]):
        # user-facing method to log the step time remaining
        step = self.getStep(key)
        self._logTimeRemaining(step)

    def _chainLogTimeRemaining(self, step: ProgressStep):
        # Log the time remaining for the step, continue to log at regular intervals
        continueToLog = self._logTimeRemaining(step)

        if continueToLog:
            updateInterval = Config["application.workflows_data.timing.logging.log_update_interval"]
            if updateInterval > 0.0:
                # if it isn't a substep, log again at regular time intervals
                if not step.isSubstep:
                    step._timer = Timer(updateInterval, self._chainLogTimeRemaining, args=[step])
                    step._timer.start()
        elif step._timer is not None:
            step._timer.cancel()
            step._timer = None

    def _logTimeRemaining(self, step: ProgressStep) -> bool:
        # Log the time remaining for an in-progress step.
        # -- returns a flag indicating whether any further logging should occur for this step.

        # The logger kwarg `extra` is used to allow precise timing information to be accessed
        #   from the generated log records when necessary.
        if step.isActive:
            continueToLog = True
            loggableName = self._loggableStepName(step.details.key, step.isSubstep)
            indent = ""
            if step.isSubstep:
                # indent substeps by the current stack depth:
                #   substeps do not "chain", so this will be the substep's stack depth.
                indent = Config["application.workflows_data.timing.logging.indent"] * (len(self._activeSteps) - 1)
            remainder = self._stepTimeRemaining(step)
            if remainder is not None:
                if remainder > 0.0:
                    logger.log(
                        Config["application.workflows_data.timing.logging.loglevel"],
                        f"{indent}{loggableName} -- estimated completion in {remainder:.1f} seconds.",
                        extra={"key": step.details.key, "dt_est": step.dt, "dt_rem": remainder},
                    )
                    if not step.isSubstep:
                        PubSubManager().publish(
                            "progress", {"step": loggableName, "dt_rem": remainder, "dt_est": step.dt}
                        )
                else:
                    logger.log(
                        Config["application.workflows_data.timing.logging.loglevel"],
                        f"{indent}{loggableName} -- is taking longer than expected.",
                        extra={"key": step.details.key, "dt_est": step.dt, "dt_rem": 0.0},
                    )
                    # Log this message only once.
                    continueToLog = False
            else:
                logger.log(
                    Config["application.workflows_data.timing.logging.loglevel"],
                    f"{indent}{loggableName} -- <no timing data is available>.",
                    extra={"key": step.details.key, "dt_est": None, "dt_rem": None},
                )
                # Log this message only once.
                continueToLog = False
            return continueToLog
        return False

    def _logCompletion(self, key: Tuple[str, ...], isSubstep: bool):
        loggableName = self._loggableStepName(key, isSubstep)
        indent = Config["application.workflows_data.timing.logging.indent"] * len(self._activeSteps)
        logger.log(Config["application.workflows_data.timing.logging.loglevel"], f"{indent}{loggableName} -- complete.")
        if not isSubstep:
            PubSubManager().publish("progress", {"step": loggableName, "dt_rem": 0.0, "dt_est": 1.0})

    def _loggableStepName(self, key: Tuple[str, ...], isSubstep: bool) -> str:
        # Remove prefix information from the logged step name when this step is a substep
        #   of another in-progress step.
        return ProgressStep.shortName(key, isSubstep)

    def stepTimeRemaining(self, key: Tuple[str | None, ...]) -> float | None:
        # Estimate the time remaining for the specified step,
        # or return `None` if not enough data is available yet to create an estimate,
        step = self.getStep(key)
        return self._stepTimeRemaining(step)

    @classmethod
    def _stepTimeRemaining(cls, step: ProgressStep) -> float | None:
        elapsed = float((datetime.now(timezone.utc) - step.startTime).total_seconds())
        remainder = None
        dt = step.dt
        if dt is not None:
            remainder = dt - elapsed
            if remainder < 0.0:
                remainder = 0.0
        return remainder

    """
    @field_validator("steps", mode="before")
    @classmethod
    def _validate_steps(cls, values: Any):
        # incoming values:
        #   either: from python: `Dict[Tuple[str, ...], ProgressStep]
        #   or:    from json: `List[Dict[<progress-step items>], ...]`: here `key: List[str, ...]`
        if isinstance(values, list):
            return {tuple(step["details"]["key"]): step for step in values}
        return values
    """

    @field_validator("steps", mode="after")
    @classmethod
    def _validate_steps(cls, steps: Any):
        # Dict[Tuple[str, ...], ProgressStep] <- List[ProgressStep] | Dict[Tuple[str | None, ...], ProgressStep]
        if isinstance(steps, list):
            return {step.details.key: step for step in steps}
        return steps

    @field_serializer("steps")
    def _serialize_steps(self, steps: Dict[Tuple[str, ...], ProgressStep], _info) -> List[ProgressStep]:
        # By default `Tuple[str, ...]` keys don't serialize correctly to JSON.
        return list(steps.values())  # *** DEBUG *** vs. `self.steps.values()` ? Which is correct?!


# The `ProgressRecorder` singleton.
ProgressRecorder = _ProgressRecorder.instance()


class WallClockTime:
    # A decorator or context manager to register a process-recording step for any method, function, or class.

    def __init__(
        self,
        stepName: str = None,
        *,
        callerOverride: FunctionType | str = None,
        N_ref: Callable[..., float] | str = lambda *_args, **_kwargs: 1.0,
        N_ref_args: Tuple[Tuple[Any, ...], Dict[str, Any]] = ((), {}),
        order: ComputationalOrder = ComputationalOrder.O_0,
        enableLogging: bool = False,
    ):
        # When used as a decorator:
        #   -- `callerOverride` is specified _only_ if the decoratee is a class:
        #      in this case it is the _name_ of the class method to be profiled;
        #   -- `stepName` may optionally be specified;
        #   -- `N_ref` may optionally be specified:
        #      it may be either a `FunctionType`, or the _name_ of a method within the
        #      decorated class;
        #   -- `N_ref_args` must not be specified;
        #   -- `order` may optionally be be specified.
        # When used as a context manager:
        #   -- `callerOverride` should not be specified;
        #   -- `stepName` must be specified;
        #   -- `N_ref`, `N_ref_args` and `order` may optionally be specified:
        #      when specified: `N_ref` must be a `FunctionType`.
        # In either case:
        #   -- when `N_ref`, `N_ref_args`, and `order` are left as `None`:
        #      at time of execution, a constant-time estimate will be used.
        # For discussion of these details, please see:
        #    `snapred.readthedocs.io/en/latest/developer/implementation_notes/profiling_and_progress_recording.html`.

        self.callerOverride = callerOverride

        # One stack frame up from the current frame: only required for use as a context manager.
        self._callingFrame: FrameType = inspect.currentframe().f_back

        self.stepName = stepName
        self.N_ref = N_ref
        self.N_ref_args = N_ref_args
        self.order = order
        self._stepKey = None
        self._enableLogging = enableLogging

    @classmethod
    def _progressRecorder(cls):
        # This is provided as a separate method to facilitate testing.
        return ProgressRecorder

    def __call__(self, decoratee: type | FunctionType) -> type | FunctionType:
        # Apply as a decorator

        # Important: the behavior of this decorator should not depend on `_ProgressRecorder.enabled`,
        #   otherwise it unnecessarily complicates any `Config` reload!
        if self.N_ref_args != ((), {}):
            raise RuntimeError(
                "Usage error: when `WallClockTime` is used as a decorator: `N_ref_args` must not be specified:\n"
                + "  these will be initialized when the decorated function is called."
            )

        func = decoratee
        if isinstance(decoratee, type):
            if not bool(self.callerOverride):
                raise RuntimeError(
                    "Usage error: on a `WallClockTime` decorated class: `callerOverride` must be specified: \n"
                    + "  this should be the name of the to-be-profiled method of the decorated class."
                )
            if not isinstance(self.callerOverride, str) or not hasattr(decoratee, self.callerOverride):
                raise RuntimeError(
                    "Usage error: on a `WallClockTime` decorated class:\n"
                    + "  `callerOverride` must be the name of a method of the decorated class."
                )

            func = getattr(decoratee, self.callerOverride)
            if not isinstance(func, FunctionType):
                raise RuntimeError(
                    "Usage error: on a `WallClockTime` decorated class:\n"
                    + "  `callerOverride` must be the name of a method of the decorated class."
                )
            # Check if `N_ref` was also given as the _name_ of a class method.
            if isinstance(self.N_ref, str):
                # Replace the N_ref name with the actual class method.
                if not hasattr(decoratee, self.N_ref):
                    raise RuntimeError(
                        "Usage error: on a `WallClockTime` decorated class:\n"
                        + "  when specified, `N_ref` may be a function,\n"
                        + "  or the the name of a method of the decorated class."
                    )
                self.N_ref = getattr(decoratee, self.N_ref)

        elif isinstance(decoratee, FunctionType):
            if bool(self.callerOverride):
                raise RuntimeError(
                    "Usage error: on a `WallClockTime` decorated function:\n"
                    + "  `callerOverride` must not be specified."
                )
            if isinstance(self.N_ref, str):
                raise RuntimeError(
                    "Usage error: on a `WallClockTime` decorated function:\n"
                    + "  when specified, `N_ref` must be a function."
                )
        else:
            raise RuntimeError(
                "Usage error: `WallClockTime` used as decorator:\n"
                + "  only `FunctionType` or `type` can be decorated."
            )

        @functools.wraps(func)  # Retain metadata from the wrapped `func`.
        def _wrapper(*args, **kwargs):
            # This wrapper profiles the call to the wrapped function using `_ProgressRecorder.record`,
            #   and `_ProgressRecorder.stop`.  In addition, it allows the `args`, and `kwargs`
            #   used by the wrapped function to be available to the `N_ref` used by the progress-reporting step.
            #   This means that in expected usage, the signature for `N_ref` should be the same as that of
            #   the wrapped function.

            try:
                if self._stepKey is not None:
                    raise RuntimeError("Usage error: a `WallClockTime` decorated function is not re-entrant.")
                self._stepKey = self._progressRecorder().record(
                    stepName=self.stepName,
                    # the decorated `FunctionType` or `type` should be used to generate the step key,
                    #   not the <class method> passed in via the `callerOverride`!
                    callerOrStackFrameOverride=decoratee,
                    N_ref=self.N_ref,
                    N_ref_args=(args, kwargs),
                    order=self.order,
                    enableLogging=self._enableLogging,
                )

                result = func(*args, **kwargs)
            finally:
                if self._stepKey is not None:
                    # IMPORTANT: `stop` needs to be called regardless of the state of `sys.exc_info()`.
                    self._progressRecorder().stop(self._stepKey)
                    self._stepKey = None
            return result

        if isinstance(decoratee, type):
            setattr(decoratee, self.callerOverride, _wrapper)
            # return the wrapped <class>
            return decoratee
        # return the wrapped <function>
        return _wrapper

    def __enter__(self):
        if not _ProgressRecorder.enabled:
            return None

        if bool(self.callerOverride) or not bool(self.stepName):
            raise RuntimeError(
                "Usage error: `WallClockTime` as context manager:\n"
                + "  `callerOverride` must not be specified;\n"
                + "  `stepName` must be specified."
            )
        if self._stepKey is not None:
            raise RuntimeError("Usage error: a `WallClockTime` context manager cannot be re-used.")

        self._stepKey = self._progressRecorder().record(
            stepName=self.stepName,
            callerOrStackFrameOverride=self._callingFrame,
            N_ref=self.N_ref,
            N_ref_args=self.N_ref_args,
            order=self.order,
            enableLogging=self._enableLogging,
        )
        return self._stepKey

    def __exit__(self, *exc):
        if not _ProgressRecorder.enabled:
            return

        if self._stepKey is not None:
            self._progressRecorder().stop(self._stepKey)
