from datetime import datetime, timezone
import inspect
import logging
import numpy as np
from pathlib import Path
from scipy.interpolate import BSpline, make_splrep
import threading

from snapred.backend.profiling.ProgressRecorder import (
    ComputationalOrder,
    _Step,
    _Measurement,
    _Estimate,
    ProgressStep,
    # The `ProgressRecorder` singleton:
    ProgressRecorder,
    # The corresponding class:
    _ProgressRecorder
)
from snapred.meta.Config import Config

from unittest import mock
import pytest
from util.Config_helpers import Config_override


class TestComputationalOrder:
    
    def test___call__(self):
        for order in ComputationalOrder:
            expected = None
            N_ref = 1.0e9 * np.random.random()
            match order:
                case ComputationalOrder.O_0:
                    expected = 1.0
                case ComputationalOrder.O_LOGN:
                    expected = np.log(N_ref)
                case ComputationalOrder.O_N:
                    expected = N_ref
                case ComputationalOrder.O_NLOGN:
                    expected = N_ref * np.log(N_ref)
                case ComputationalOrder.O_N_2:
                    expected = N_ref**(2.0)
                case ComputationalOrder.O_N_3:
                    expected = N_ref**(3.0)

            actual = order(N_ref)
            assert actual == pytest.approx(expected, rel=1.0e-9)

    
class Test_Step:
        
    class _Class:
    
        def method(self):
            pass
            
        def another_method(self):
            # do something besides `pass`:
            self._two = 1 + 1
        
        def same_as_another_method(self):
            # match bytecode from `another_method`
            self._two = 1 + 1
           
    def test__callable_hash(self):
        # Only allow hashes for `FunctionType`:

        def _function():
            pass
    
        _class = Test_Step._Class()
        
        # `FunctionType`
        h0 = _Step._callable_hash(_function) # `FunctionType`
        assert isinstance(h0, str)
        assert len(h0) == 16
        
        # `FunctionType` <- `LambdaType`
        h1 = _Step._callable_hash(lambda *_args, **kwargs: args[1] * args[1]) # `FunctionType`
        assert isinstance(h0, str)
        assert len(h0) == 16
        
        # `FunctionType`
        h2 = _Step._callable_hash(Test_Step._Class.method)
        assert isinstance(h0, str)
        assert len(h0) == 16
        
        # `MethodType`
        with pytest.raises(RuntimeError, match="Usage error: expecting `FunctionType`.*"):
            _Step._callable_hash(_class.method) # the _bound_ method
        
        # type `Test_Step._Class`
        with pytest.raises(RuntimeError, match="Usage error: expecting `FunctionType`.*"):
            _Step._callable_hash(_class) # a class instance
        
        # `type`
        with pytest.raises(RuntimeError, match="Usage error: expecting `FunctionType`.*"):
            _Step._callable_hash(Test_Step._Class) # a class
            
    def test__callable_hash_hashes_persist(self):
        # hash values don't change
        
        def _function():
            pass
    
        _class = Test_Step._Class()
        
        h0 = _Step._callable_hash(_function) # `FunctionType`
        h1 = _Step._callable_hash(lambda *_args, **kwargs: args[1] * args[1]) # `FunctionType`
        h2 = _Step._callable_hash(Test_Step._Class.method)

        assert h0 == _Step._callable_hash(_function)
        assert h1 == _Step._callable_hash(lambda *_args, **kwargs: args[1] * args[1])
        assert h2 == _Step._callable_hash(Test_Step._Class.method)
            
    def test__callable_hash_hashes_distinct(self):
        # hash values are distinct
        
        def _function():
            pass
    
        _class = Test_Step._Class()
        
        h0 = _Step._callable_hash(_function) # `FunctionType`
        h1 = _Step._callable_hash(lambda *_args, **kwargs: args[1] * args[1]) # `FunctionType`
        h2 = _Step._callable_hash(Test_Step._Class.method)

        assert h0 != h1
        assert h1 != h2
        # Note here that `h0 == h2`, because their bytecodes match.
            
    def test__callable_hash_same_bytecodes(self):
        # hash values from matching bytecodes are the same
        
        def _function():
            pass
    
        _class = Test_Step._Class()
        
        assert _Step._callable_hash(_function) == _Step._callable_hash(Test_Step._Class.method)
        assert _Step._callable_hash(Test_Step._Class.another_method)\
                   == _Step._callable_hash(Test_Step._Class.same_as_another_method)
        assert _Step._callable_hash(_function)\
                   != _Step._callable_hash(Test_Step._Class.another_method)

    def test_init(self):
        key = ('one.two.three', 'four.five', None)
        
        def N_ref(*args, **kwargs):
            return args[0] * args[1] * len(kwargs)
        N_ref_hash = _Step._callable_hash(N_ref)
            
        # `key` arg is required, all others are optional
        s0 = _Step(key)
        with pytest.raises(TypeError, match=".*missing 1 required positional argument: 'key'.*"):
            s0 = _Step()
        
        # No default values should be set:
        # -- it's important that this happens after the `__init__`,
        #    otherwise there's no way to determine if `N_ref` for a `_Step`
        #    has been changed!
        s0 = _Step(key)
        assert s0._N_ref is None
        assert s0.N_ref_hash is None
        assert s0._N_ref_args == ((), {})
        assert s0.order is None
        
        # Setting `N_ref` should set `N_ref_hash`.
        s1 = _Step(key, N_ref=N_ref)
        assert s1._N_ref == N_ref
        assert isinstance(s1.N_ref_hash, str)
        assert len(s1.N_ref_hash) == 16
        
        # Both shouldn't be passed in at the same time.
        with pytest.raises(RuntimeError, match="Usage error:.*specify either `N_ref` or `N_ref_hash`, but not both."):
            s2 = _Step(key, N_ref=N_ref, N_ref_hash="abcdeeeeffff0000")
        
        # Both shouldn't be passed in at the same time, even if they are consistent.
        with pytest.raises(RuntimeError, match="Usage error:.*specify either `N_ref` or `N_ref_hash`, but not both."):
            s3 = _Step(key, N_ref=N_ref, N_ref_hash=s1.N_ref_hash)

    def test_default(self):
        key = ('one.two.three', 'four.five', None)
        # No default values should be set:
        # -- it's important that this happens after the `__init__`,
        #    otherwise there's no way to determine if `N_ref` for a `_Step`
        #    has been changed!
        s0 = _Step.default(key)
        assert s0._N_ref is None
        assert s0.N_ref_hash is None
        assert s0._N_ref_args == ((), {})
        assert s0.order is None
        
    def test_setDetails(self, caplog):
        key = ('one.two.three', 'four.five', None)
        
        # Attributes should only be set when the corresponding arg is not `None`.
        # N_ref, N_ref_args, order
        s = _Step(key)
        assert s._N_ref is None
        assert s._N_ref_args == ((), {})
        assert s.order is None
        
        # Attributes should only be set when the corresponding arg is not `None`.
        # .. set `_N_ref` => `_N_ref_args` and `order` remain the same.
        s = _Step(key)
        original_args = ((1, 2), {"1": "one", "2": "two"})
        original_order = ComputationalOrder.O_N
        s._N_ref_args = original_args
        s.order = original_order
        s.setDetails(N_ref=lambda *_args, **_kwargs: 1.0)
        assert s._N_ref is not None
        assert s._N_ref_args == original_args
        assert s.order == original_order
        
        # .. set `order` => `_N_ref` and `N_ref_args` remain the same.
        s = _Step(
                key,
                N_ref=lambda *args, **kwargs: args[0] * args[1] * len(kwargs),
                N_ref_args=((1.0, 2.0), {"one": 1, "two": 2})
            )
        original_hash = s.N_ref_hash
        original_args = s._N_ref_args
        s.setDetails(order=ComputationalOrder.O_N_2)
        assert s.order is ComputationalOrder.O_N_2
        assert s.N_ref_hash == original_hash
        assert s._N_ref_args == original_args
        
        # When `N_ref_hash` had a previous value, log a message when it changes.
        original_N_ref = lambda *args, **kwargs: args[0] * args[1] * len(kwargs)
        original_hash = _Step._callable_hash(original_N_ref)
        new_N_ref = lambda *args, **kwargs: float(len(args)) * len(kwargs)
        new_hash = _Step._callable_hash(new_N_ref)
        
        # .. if a step has been loaded from persistent data: only its `N_ref_hash` will be set.
        s = _Step(
                key,
                N_ref_hash=original_hash
            )
        assert s._N_ref is None
        assert s.N_ref_hash == original_hash
        with caplog.at_level(logging.WARNING, logger=inspect.getmodule(ProgressRecorder).__name__):
            s.setDetails(N_ref=new_N_ref)
        assert f"`N_ref` for step {key} has been modified" in caplog.text            
        assert s.N_ref_hash == new_hash
        
        # In other situations, both `_N_ref` and `N_ref_hash` will be set.
        s = _Step(
                key,
                N_ref=original_N_ref
            )
        assert s.N_ref_hash == original_hash
        assert s._N_ref is not None
        with caplog.at_level(logging.WARNING, logger=inspect.getmodule(ProgressRecorder).__name__):
            s.setDetails(N_ref=new_N_ref)
        assert f"`N_ref` for step {key} has been modified" in caplog.text            
        assert s.N_ref_hash == new_hash
        
class Test_Estimate:

    def test_init(self):
        SPLINE_DEGREE = _Estimate.SPLINE_DEGREE
        assert SPLINE_DEGREE == Config["application.workflows_data.timing.spline_order"]
    
        # all args are optional
        e = _Estimate()
        assert e.count is None
        assert e.tck is None
        
        # `tck` `k` arg must be <= SPLINE_DEGREE.
        Ns = np.linspace(0.0, 1.0e9, 20)
        dts = np.linspace(0.0, 60.0, 20)
        spl = make_splrep(Ns, dts**2.7, k=SPLINE_DEGREE + 1)
        with pytest.raises(ValueError, match=f"In `tck`, specified spline degree `k` must be <= {SPLINE_DEGREE}."):
            e = _Estimate(tck=(list(spl.tck[0]), list(spl.tck[1]), spl.tck[2]))
        
        # `tck` `k` arg may be _zero_ => a constant-time estimate will be applied.
        Ns = np.linspace(0.0, 1.0e9, 20)
        dts = np.ones_like(Ns)
        spl = make_splrep(Ns, dts, k=0)
        e = _Estimate(tck=(list(spl.tck[0]), list(spl.tck[1]), spl.tck[2]))

    def test_default(self):
        # returns a _linear_ estimator (degree `k == 1`):
        e = _Estimate.default()
        assert e._spl is not None
        assert e.tck[2] == 1
        
        # verify estimate at `N == 1.0e-9`: should be 60.0s
        assert 60.0 == pytest.approx(e.dt(1.0e9), rel=1.0e-3)
        # verify estimate at `N == 5.0e-9`: should be 300.0s
        assert 300.0 == pytest.approx(e.dt(5.0e9), rel=1.0e-3)
    
    def test_dt(self):
        # check update before evaluation usage exception
        e = _Estimate()
        with pytest.raises(RuntimeError, match="Usage error: `dt` called before `update`."):
            e.dt(1.0e9)
        
        # returns `float`, not `np.float64` or `np.ndarray`:
        e = _Estimate.default()
        isinstance(e.dt(1.0e9), float)
        
    def test_update(self):
        Ns = np.linspace(0.0, 1.0e9, 20)
        dts = Ns**(2.7)
        measurements = [_Measurement(dt=dts[n], N_ref=Ns[n]) for n in range(len(Ns))]
        instance = _Estimate.default()
        
        # `update` calls `_prepareData` and `_update`
        with (
            mock.patch.object(instance, "_prepareData") as mock_prepareData,
            mock.patch.object(instance, "_update") as mock_update
        ):
            mock_prepareData.return_value = (Ns, dts)
            instance.update(measurements, ComputationalOrder.O_N_2)
            mock_prepareData.assert_called_once_with(measurements, ComputationalOrder.O_N_2)
            mock_update.assert_called_once_with(Ns, dts, len(measurements))

    def test__update(self):
        # doesn't do anything if there are no data points
        e = _Estimate.default()
        with mock.patch.object(inspect.getmodule(ProgressRecorder), "make_splrep") as mock_make_splrep:
            e._update(np.array([]), np.array([]), 0)
            mock_make_splrep.assert_not_called()

        with Config_override("application.workflows_data.timing.spline_order", 3):
            SPLINE_DEGREE = _Estimate.SPLINE_DEGREE
            
            # provides constant-time estimate when necessary (only one measurement):
            SPLINE_DEGREE = _Estimate.SPLINE_DEGREE
            e = _Estimate.default()
            Ns, dts = np.array([1.0e9]), np.array([60.0])
            e._update(Ns, dts, len(Ns))
            assert e.tck[2] == 0

            # provides linear estimate when necessary (only two measurements):
            e = _Estimate.default()
            Ns, dts = np.array([1.0e9, 2.0e9]), np.array([60.0, 120.0])
            e._update(Ns, dts, len(Ns))
            assert e.tck[2] == 1

            # .. quadratic estimate:
            e = _Estimate.default()
            Ns, dts = np.array([1.0e9, 2.0e9, 2.2e9]), np.array([60.0, 120.0, 180.0])
            e._update(Ns, dts, len(Ns))
            assert e.tck[2] == 2

            # .. cubic estimate:
            e = _Estimate.default()
            Ns, dts = np.array([1.0e9, 2.0e9, 2.2e9, 3.0e9]), np.array([60.0, 120.0, 180.0, 240.0])
            e._update(Ns, dts, len(Ns))
            assert e.tck[2] == 3

            # When more data points are available, limits degree of estimate to SPLINE_DEGREE.
            e = _Estimate.default()
            Ns, dts = np.array([1.0e9, 2.0e9, 2.2e9, 3.0e9, 3.5e9]), np.array([60.0, 120.0, 180.0, 240.0, 300.0])
            e._update(Ns, dts, len(Ns))
            assert e.tck[2] == 3

    def test__prepareData(self):
        # either all `measurement.N_ref` are `None` or all are `float`:
        Ns =  np.linspace(0.0, 1.0e9, 20)
        dts = Ns**(2.7)
        #   convert `Ns` to list, so that it can have `None` elements
        Ns = list(Ns)
        Ns[3], Ns[10], Ns[11] = None, None, None
        measurements = [_Measurement(dt=dts[n], N_ref=Ns[n]) for n in range(len(Ns))]

        with pytest.raises(
                RuntimeError,
                match="Either all of the `N_ref` values should be `float`, or all should be `None`."
            ):
            e = _Estimate.default()
            e._prepareData(measurements, ComputationalOrder.O_N_2)
        
        # inserts "tie point" at (0.0, 0.0) when required:
        Ns = np.linspace(10.0, 1.0e9, 20)
        dts = Ns**(2.7)
        assert Ns[0] != 0.0
        assert dts[0] != 0.0
        measurements = [_Measurement(dt=dts[n], N_ref=Ns[n]) for n in range(len(Ns))]
        e = _Estimate.default()
        Ns_, dts_ = e._prepareData(measurements, ComputationalOrder.O_N_2)
        assert len(Ns_) == len(Ns) + 1
        assert len(dts_) == len(dts) + 1
        assert Ns_[0] == 0.0
        assert dts_[0] == 0.0

        # calls _accumulateDuplicates
        Ns = np.linspace(0.0, 1.0e9, 20)
        dts = Ns**(2.7)        
        measurements = [_Measurement(dt=dts[n], N_ref=Ns[n]) for n in range(len(Ns))]
        instance = _Estimate.default()
        with mock.patch.object(instance, "_accumulateDuplicates") as mock_accumulateDuplicates:
            mock_accumulateDuplicates.return_value = (Ns, dts)
            Ns_, dts_ = instance._prepareData(measurements, ComputationalOrder.O_N_2)
            mock_accumulateDuplicates.assert_called_once()
        
        # .. returns `np.ndarray, np.ndarray`
        assert isinstance(Ns_, np.ndarray)
        assert isinstance(dts_, np.ndarray)

    def test__accumulateDuplicates(self):
        # works with `[(None, dt), ...]`:
        Ns =  np.linspace(0.0, 1.0e9, 20)
        dts = Ns**(2.7)
        ps = [(None, dt) for dt in dts]
        e = _Estimate.default()
        Ns_, dts_ = e._accumulateDuplicates(ps)
        assert len(Ns_) == len(dts_)
        assert Ns_ == [None]
        assert len(dts_) == 1
        # `N_ref is None` => constant-time estimate, so `_accumulateDuplicates` will return the mean. 
        assert dts_[0] == pytest.approx(sum(dts) / len(dts), rel=1.0e-3)
        
        # when there are no duplicates: it doesn't change the data
        Ns =  np.linspace(0.0, 1.0e9, 20)
        dts = Ns**(2.7)
        ps = [(N, dt) for N, dt in zip(Ns, dts)]
        e = _Estimate.default()
        Ns_, dts_ = e._accumulateDuplicates(ps)
        assert len(Ns_) == len(dts_)
        assert Ns.tolist() == list(Ns_)
        # values should be unmodified: use exact comparison
        assert dts_ == list(dts_)
        
        # accumulates duplicates as expected
        Ns = np.array([1.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 3.0, 3.0])
        dts = Ns**2.5 + 0.1 * np.random.random(size=Ns.shape)
        ps = [(N, dt) for N, dt in zip(Ns, dts)]
        e = _Estimate.default()
        Ns_, dts_ = e._accumulateDuplicates(ps)
        assert len(Ns_) == len(dts_)
        assert [1.0, 2.0, 3.0] == list(Ns_)
        assert [dts[0:3].mean(), dts[3:5].mean(), dts[5:].mean()] == pytest.approx(list(dts_), rel=1.0e-6)

        # works with mixtures of duplicated and non-duplicated values
        Ns = np.array([1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 5.0])
        dts = Ns**0.8 + 0.5 * np.random.random(size=Ns.shape)
        ps = [(N, dt) for N, dt in zip(Ns, dts)]
        e = _Estimate.default()
        Ns_, dts_ = e._accumulateDuplicates(ps)
        assert len(Ns_) == len(dts_)
        assert [1.0, 2.0, 3.0, 4.0, 5.0] == list(Ns_)
        assert [dts[0:3].mean(), dts[3], dts[4], dts[5], dts[6:].mean()] == pytest.approx(list(dts_), rel=1.0e-6)
        
        Ns = np.array([1.0, 2.0, 3.0, 4.0, 4.0, 4.0, 5.0, 6.0, 7.0])
        dts = Ns**3.1 + 0.01 * np.random.random(size=Ns.shape)
        ps = [(N, dt) for N, dt in zip(Ns, dts)]
        e = _Estimate.default()
        Ns_, dts_ = e._accumulateDuplicates(ps)
        assert len(Ns_) == len(dts_)
        assert [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0] == list(Ns_)
        assert [dts[0], dts[1], dts[2], dts[3:6].mean(), dts[6], dts[7], dts[8]] == pytest.approx(list(dts_), rel=1.0e-6)

class TestProgressStep:
    @pytest.fixture(autouse=True)
    def _setUpTest(self):
        # setup:
        
        self.key = ("module.name", "qualified.function.name", "step-name")
        Ns = [1.0, 2.0, 3.0]
        dts = [45.0, 75.0, 85.2]
        measurements = [_Measurement(dt=dts[n], N_ref=Ns[n]) for n in range(len(Ns))]
        self.s = ProgressStep(
                     details=_Step(self.key),
                     measurements=measurements,
                     estimate=_Estimate.default()
                 )
        yield
        
        # teardown
        pass
        
    def test_init(self):    
        # verify that private attributes have been initialized correctly
        assert self.s._startTime is None
        assert self.s._loggingEnabled == False
        assert self.s._timer == None

    def test_setDetails(self):
        # verify that `setDetails` works correctly
        N_ref = lambda *args, **kwargs: args[0] * args[1] * len(kwargs)
        N_ref_args = ((1, 2), {"three": 3})
        order = ComputationalOrder.O_N_3
        enableLogging = True

        with mock.patch.object(self.s, "details") as mockDetails:
            # sets `_loggingEnabled`
            assert not self.s._loggingEnabled
            self.s.setDetails(
                N_ref=N_ref,
                N_ref_args=N_ref_args,
                order=order,
                enableLogging=enableLogging
            )
            assert self.s._loggingEnabled == enableLogging
            
            # calls `self.details.setDetails`
            mockDetails.setDetails.assert_called_once_with(
                N_ref=N_ref,
                N_ref_args=N_ref_args,
                order=order
            )           

    def test_setDetails_defaults(self):
        # verify that `setDetails` works correctly RE default args
        assert not self.s._loggingEnabled
        with mock.patch.object(self.s, "details") as mockDetails:
            self.s.setDetails()
            mockDetails.setDetails.assert_called_once_with(
                N_ref=None,
                N_ref_args=None,
                order=None
            )
            assert not self.s._loggingEnabled

    def test_setDetails_enable_logging(self):
        # verify that `setDetails(enableLogging=<flag>)` works correctly
        assert not self.s._loggingEnabled
        with mock.patch.object(self.s, "details") as mockDetails:
            self.s.setDetails(enableLogging=True)
            mockDetails.setDetails.assert_called_once_with(
                N_ref=None,
                N_ref_args=None,
                order=None,
            )
            assert self.s._loggingEnabled

            self.s.setDetails(enableLogging=False)
            assert not self.s._loggingEnabled
            
    def test_start(self):
        _now = datetime.now(timezone.utc)
        with mock.patch.object(inspect.getmodule(ProgressRecorder), "datetime") as mock_datetime:
            mock_datetime.now = mock.Mock(return_value=_now)
            
            assert not self.s._loggingEnabled
            self.s.start()

            # verify that the utc-timezone is used
            mock_datetime.now.assert_called_once_with(timezone.utc)

            # verify that _startTime is set
            assert self.s._startTime == _now

            # verify that other args aren't affected
            assert self.s._timer is None
            assert not self.s._loggingEnabled

    def test_stop(self):
        expected = datetime.now(timezone.utc)
        self.s._startTime = expected
        mockTimer = mock.Mock(spec=threading.Timer)
        self.s._timer = mockTimer

        actual = self.s.stop()

        # clears `_startTime` and `_timer`:
        assert self.s._startTime is None
        assert self.s._timer is None

        # cancels the `_timer`
        mockTimer.cancel.assert_called_once()

        # returns the `_startTime`
        assert expected == actual

    def test_stop_no_timer(self):
        expected = datetime.now(timezone.utc)
        self.s._startTime = expected
        assert self.s._timer is None

        actual = self.s.stop()

        # clears `_startTime`:
        assert self.s._startTime is None
        assert self.s._timer is None

        # returns the `_startTime`
        assert expected == actual

    def test_stop_not_started(self):
        # checks that the step has been started
        assert self.s._startTime is None
        with pytest.raises(RuntimeError, match="Usage error: attempt to `stop` unstarted step.*"):
            self.s.stop()

    def test_name(self):
        # calls `humanReadableName`
        with mock.patch.object(ProgressStep, "humanReadableName") as mockHumanReadableName:
            name = self.s.name
            mockHumanReadableName.assert_called_once_with(self.key)

    def test_humanReadableName(self):            
        # excludes `None` fields
        key = (None, "lah.dee.dah", None)
        assert ProgressStep.humanReadableName(key) == key[1]
        
        key = ("fiddle", None, "dee.dee")
        assert ProgressStep.humanReadableName(key) == ".".join((key[0], key[2]))

        # includes all non-trivial fields
        key = ("one.two.three", "four.five", "six")
        assert ProgressStep.humanReadableName(key) == ".".join(key)

    def test_startTime(self):
        _now = datetime.now(timezone.utc)

        # exception if the step hasn't been started
        with pytest.raises(RuntimeError, match="Usage error: attempt to read `startTime` for unstarted step.*"):
            _time = self.s.startTime

        # returns `_startTime`     
        self.s._startTime = _now
        assert self.s.startTime == _now

    def test_isActive(self):
        _now = datetime.now(timezone.utc)

        # returns False if step hasn't been started
        assert self.s._startTime is None
        assert not self.s.isActive

        # returns True if step has been started
        self.s._startTime = _now
        assert self.s.isActive

    def test_loggingEnabled(self):
        _now = datetime.now(timezone.utc)

        # returns _loggingEnabled
        assert not self.s._loggingEnabled
        assert not self.s.loggingEnabled

        self.s._loggingEnabled = True
        assert self.s.loggingEnabled


class TestProgressRecorder:
    # pytest-style: uses fixtures

    @pytest.fixture(autouse=True, scope="class")
    @classmethod
    def _setup_test_class(cls, Config_override_fixture):
        # setup
        
        # The `ProgressRecorder` singleton has been created at import of its module.
        #   Since it is by-default _disabled_ in the test environment, it contains
        #   no persistent data; however, for all of these tests it now needs to be enabled:
        Config_override_fixture("application.workflows_data.timing.enable", True)
        yield
        
        # teardown
        pass
        
    @pytest.fixture(autouse=True)
    def _setup_test(self):
        # setup
        
        # Use a temporary directory for "user.application.data.home":
        Config_override("user.application.data.home", tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/user.application.data.home_")))
        yield
        
        # teardown
        
        # Re-initialize the `ProgressRecorder` at the end of each test.
        ProgressRecorder.steps.clear()

    @pytest.fixture()
    def _init_with_data(self, data: str) -> mock.MagicMock:
        # Initialize the `ProgressRecorder` singleton from its JSON representation.
        # returns: a wrapped version of the `ProgressRecorder` instance.
        
        def _init() -> mock.MagicMock:
            # setup
            
            instance = _ProgressRecorder.model_validate_json(data)
            mockSingleton = mock.patch.object(inspect.getmodule(ProgressRecorder), "ProgressRecorder", instance)
            mockSingleton.start()                   
            yield mockSingleton
            
            # teardown
            mockSingleton.stop()
        
        return _init()
