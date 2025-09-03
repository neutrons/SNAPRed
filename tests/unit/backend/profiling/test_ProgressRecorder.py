import inspect
import logging
import re
import sys
import tempfile
import threading
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from time import sleep
from types import FrameType, FunctionType, LambdaType
from unittest import mock

import numpy as np
import pytest
from scipy.interpolate import make_splrep
from util.Config_helpers import Config_override

from snapred.backend.profiling.ProgressRecorder import (
    ComputationalOrder,
    # The `ProgressRecorder` singleton:
    ProgressRecorder,
    ProgressStep,
    # The decorator / context manager:
    WallClockTime,
    _Estimate,
    _Measurement,
    # The corresponding class:
    _ProgressRecorder,
    _Step,
)
from snapred.meta.Config import Config, Resource


class TestComputationalOrder:
    def test___call__(self):
        for order in ComputationalOrder:
            expected = None
            N_ref = 1.0e9 * np.random.random()
            match order:
                case ComputationalOrder.O_0:
                    expected = 1.0
                case ComputationalOrder.O_LOG_N:
                    expected = np.log(N_ref)
                case ComputationalOrder.O_N:
                    expected = N_ref
                case ComputationalOrder.O_N_LOG_N:
                    expected = N_ref * np.log(N_ref)
                case ComputationalOrder.O_N_2:
                    expected = N_ref ** (2.0)
                case ComputationalOrder.O_N_3:
                    expected = N_ref ** (3.0)

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
        h0 = _Step._callable_hash(_function)  # `FunctionType`
        assert isinstance(h0, str)
        assert len(h0) == 16

        # `FunctionType` <- `LambdaType`
        h1 = _Step._callable_hash(lambda *args, **_kwargs: args[1] * args[1])  # `FunctionType`
        assert isinstance(h1, str)
        assert len(h1) == 16

        # `FunctionType`
        h2 = _Step._callable_hash(Test_Step._Class.method)
        assert isinstance(h2, str)
        assert len(h2) == 16

        # `MethodType`
        with pytest.raises(RuntimeError, match="Usage error: expecting `FunctionType`.*"):
            _Step._callable_hash(_class.method)  # the _bound_ method

        # type `Test_Step._Class`
        with pytest.raises(RuntimeError, match="Usage error: expecting `FunctionType`.*"):
            _Step._callable_hash(_class)  # a class instance

        # `type`
        with pytest.raises(RuntimeError, match="Usage error: expecting `FunctionType`.*"):
            _Step._callable_hash(Test_Step._Class)  # a class

    def test__callable_hash_hashes_persist(self):
        # hash values don't change

        def _function():
            pass

        _class = Test_Step._Class()

        h0 = _Step._callable_hash(_function)  # `FunctionType`
        h1 = _Step._callable_hash(lambda *args, **_kwargs: args[1] * args[1])  # `FunctionType`
        h2 = _Step._callable_hash(Test_Step._Class.method)

        assert h0 == _Step._callable_hash(_function)
        assert h1 == _Step._callable_hash(lambda *args, **_kwargs: args[1] * args[1])
        assert h2 == _Step._callable_hash(Test_Step._Class.method)

    def test__callable_hash_hashes_distinct(self):
        # hash values are distinct

        def _function():
            pass

        _class = Test_Step._Class()

        h0 = _Step._callable_hash(_function)  # `FunctionType`
        h1 = _Step._callable_hash(lambda *args, **_kwargs: args[1] * args[1])  # `FunctionType`
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
        assert _Step._callable_hash(Test_Step._Class.another_method) == _Step._callable_hash(
            Test_Step._Class.same_as_another_method
        )
        assert _Step._callable_hash(_function) != _Step._callable_hash(Test_Step._Class.another_method)

    def test_init(self):
        key = ("one.two.three", "four.five", None)

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
        assert s1.N_ref_hash == N_ref_hash

        # Both shouldn't be passed in at the same time.
        with pytest.raises(RuntimeError, match="Usage error:.*specify either `N_ref` or `N_ref_hash`, but not both."):
            s2 = _Step(key, N_ref=N_ref, N_ref_hash="abcdeeeeffff0000")  # noqa: F841

        # Both shouldn't be passed in at the same time, even if they are consistent.
        with pytest.raises(RuntimeError, match="Usage error:.*specify either `N_ref` or `N_ref_hash`, but not both."):
            s3 = _Step(key, N_ref=N_ref, N_ref_hash=s1.N_ref_hash)  # noqa: F841

    def test_default(self):
        key = ("one.two.three", "four.five", None)
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
        key = ("one.two.three", "four.five", None)

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
            N_ref_args=((1.0, 2.0), {"one": 1, "two": 2}),
        )
        original_hash = s.N_ref_hash
        original_args = s._N_ref_args
        s.setDetails(order=ComputationalOrder.O_N_2)
        assert s.order is ComputationalOrder.O_N_2
        assert s.N_ref_hash == original_hash
        assert s._N_ref_args == original_args

        # When `N_ref_hash` had a previous value, log a message when it changes.
        original_N_ref = lambda *args, **kwargs: args[0] * args[1] * len(kwargs)  # noqa: E731
        original_hash = _Step._callable_hash(original_N_ref)
        new_N_ref = lambda *args, **kwargs: float(len(args)) * len(kwargs)  # noqa: E731
        new_hash = _Step._callable_hash(new_N_ref)

        # .. if a step has been loaded from persistent data: only its `N_ref_hash` will be set.
        s = _Step(key, N_ref_hash=original_hash)
        assert s._N_ref is None
        assert s.N_ref_hash == original_hash
        with caplog.at_level(logging.WARNING, logger=inspect.getmodule(ProgressRecorder).__name__):
            s.setDetails(N_ref=new_N_ref)
        assert f"`N_ref` for step {key} has been modified" in caplog.text
        assert s.N_ref_hash == new_hash

        # In other situations, both `_N_ref` and `N_ref_hash` will be set.
        s = _Step(key, N_ref=original_N_ref)
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

        # verify estimate at `N == 1.0e-9`: should be 3.0s
        assert 3.0 == pytest.approx(e.dt(1.0e9), rel=1.0e-3)
        # verify estimate at `N == 5.0e-9`: should be 15.0s
        assert 15.0 == pytest.approx(e.dt(5.0e9), rel=1.0e-3)

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
        dts = Ns ** (2.7)
        measurements = [_Measurement(dt=dts[n], dt_est=dts[n] * 2.0, N_ref=Ns[n]) for n in range(len(Ns))]
        instance = _Estimate.default()

        # `update` calls `_prepareData` and `_update`
        with (
            mock.patch.object(instance, "_prepareData") as mock_prepareData,
            mock.patch.object(instance, "_update") as mock_update,
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
            # provides constant-time estimate when necessary (only one measurement):
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
        Ns = np.linspace(0.0, 1.0e9, 20)
        dts = Ns ** (2.7)
        #   convert `Ns` to list, so that it can have `None` elements
        Ns = list(Ns)
        Ns[3], Ns[10], Ns[11] = None, None, None
        measurements = [_Measurement(dt=dts[n], dt_est=dts[n] * 2.0, N_ref=Ns[n]) for n in range(len(Ns))]

        with pytest.raises(  # noqa: PT012
            RuntimeError, match="Either all of the `N_ref` values should be `float`, or all should be `None`."
        ):
            e = _Estimate.default()
            e._prepareData(measurements, ComputationalOrder.O_N_2)

        # inserts "tie point" at (0.0, 0.0) when required:
        Ns = np.linspace(10.0, 1.0e9, 20)
        dts = Ns ** (2.7)
        assert Ns[0] != 0.0
        assert dts[0] != 0.0
        measurements = [_Measurement(dt=dts[n], dt_est=dts[n] * 2.0, N_ref=Ns[n]) for n in range(len(Ns))]
        e = _Estimate.default()
        Ns_, dts_ = e._prepareData(measurements, ComputationalOrder.O_N_2)
        assert len(Ns_) == len(Ns) + 1
        assert len(dts_) == len(dts) + 1
        assert Ns_[0] == 0.0
        assert dts_[0] == 0.0

        # calls _accumulateDuplicates
        Ns = np.linspace(0.0, 1.0e9, 20)
        dts = Ns ** (2.7)
        measurements = [_Measurement(dt=dts[n], dt_est=dts[n] * 2.0, N_ref=Ns[n]) for n in range(len(Ns))]
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
        Ns = np.linspace(0.0, 1.0e9, 20)
        dts = Ns ** (2.7)
        ps = [(None, dt) for dt in dts]
        e = _Estimate.default()
        Ns_, dts_ = e._accumulateDuplicates(ps)
        assert len(Ns_) == len(dts_)
        assert Ns_ == [None]
        assert len(dts_) == 1
        # `N_ref is None` => constant-time estimate, so `_accumulateDuplicates` will return the mean.
        assert dts_[0] == pytest.approx(sum(dts) / len(dts), rel=1.0e-3)

        # when there are no duplicates: it doesn't change the data
        Ns = np.linspace(0.0, 1.0e9, 20)
        dts = Ns ** (2.7)
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
        assert [dts[0], dts[1], dts[2], dts[3:6].mean(), dts[6], dts[7], dts[8]] == pytest.approx(
            list(dts_), rel=1.0e-6
        )


class TestProgressStep:
    @pytest.fixture(autouse=True)
    def _setUpTest(self):
        # setup:

        self.key = ("module.name", "qualified.function.name", "step-name")
        Ns = [1.0, 2.0, 3.0]
        dts = [45.0, 75.0, 85.2]
        measurements = [_Measurement(dt=dts[n], dt_est=dts[n] * 2.0, N_ref=Ns[n]) for n in range(len(Ns))]
        self.s = ProgressStep(details=_Step(self.key), measurements=measurements, estimate=_Estimate.default())
        yield

        # teardown
        pass

    def test_init(self):
        # verify that private attributes have been initialized correctly
        assert self.s._isSubstep == False  # noqa: E712
        assert self.s._startTime is None
        assert self.s._loggingEnabled == False  # noqa: E712
        assert self.s._timer is None

    def test_setDetails(self):
        # verify that `setDetails` works correctly
        N_ref = lambda *args, **kwargs: args[0] * args[1] * len(kwargs)  # noqa: E731
        N_ref_args = ((1, 2), {"three": 3})
        order = ComputationalOrder.O_N_3
        enableLogging = True

        with mock.patch.object(self.s, "details") as mockDetails:
            # sets `_loggingEnabled`
            assert not self.s._loggingEnabled
            self.s.setDetails(N_ref=N_ref, N_ref_args=N_ref_args, order=order, enableLogging=enableLogging)
            assert self.s._loggingEnabled == enableLogging

            # calls `self.details.setDetails`
            mockDetails.setDetails.assert_called_once_with(N_ref=N_ref, N_ref_args=N_ref_args, order=order)

    def test_setDetails_defaults(self):
        # verify that `setDetails` works correctly RE default args
        assert not self.s._loggingEnabled
        with mock.patch.object(self.s, "details") as mockDetails:
            self.s.setDetails()
            mockDetails.setDetails.assert_called_once_with(N_ref=None, N_ref_args=None, order=None)
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
        with (
            mock.patch.object(inspect.getmodule(ProgressRecorder), "datetime") as mock_datetime,
            mock.patch.object(_Step, "N_ref", return_value=mock.sentinel.N_ref) as mock_N_ref,
            mock.patch.object(_Estimate, "dt") as mock_dt,
        ):
            mock_order = mock.Mock(return_value=mock.sentinel.N)  # placed here to patch the `BaseModel` attribute
            self.s.details.order = mock_order
            mock_datetime.now = mock.Mock(return_value=_now)
            mock_N_ref.return_value = mock.sentinel.N_ref
            mock_dt.return_value = mock.sentinel.dt

            assert not self.s._loggingEnabled
            self.s.start(isSubstep=False)

            # verify that the utc-timezone is used
            mock_datetime.now.assert_called_once_with(timezone.utc)

            # verify that _isSubstep is not set
            assert self.s._isSubstep == False  # noqa: E712

            # verify that _startTime is set
            assert self.s._startTime == _now

            # verify that the timing properties have been initialized correctly
            assert self.s._N_ref == mock.sentinel.N_ref
            assert self.s._dt == mock.sentinel.dt
            mock_order.assert_called_once_with(mock.sentinel.N_ref)
            mock_dt.assert_called_once_with(mock.sentinel.N)

            # verify that other args aren't affected
            assert self.s._timer is None
            assert not self.s._loggingEnabled

    def test_start_substep(self):
        _now = datetime.now(timezone.utc)
        with (
            mock.patch.object(inspect.getmodule(ProgressRecorder), "datetime") as mock_datetime,
            mock.patch.object(_Step, "N_ref", return_value=mock.sentinel.N_ref) as mock_N_ref,
            mock.patch.object(_Estimate, "dt") as mock_dt,
        ):
            mock_order = mock.Mock(return_value=mock.sentinel.N)  # placed here to patch the `BaseModel` attribute
            self.s.details.order = mock_order
            mock_datetime.now = mock.Mock(return_value=_now)
            mock_N_ref.return_value = mock.sentinel.N_ref
            mock_dt.return_value = mock.sentinel.dt

            assert not self.s._loggingEnabled
            self.s.start(isSubstep=True)

            # verify that the utc-timezone is used
            mock_datetime.now.assert_called_once_with(timezone.utc)

            # verify that _isSubstep is set
            assert self.s._isSubstep == True  # noqa: E712

            # verify that _startTime is set
            assert self.s._startTime == _now

            # verify that the timing properties have been initialized correctly
            assert self.s._N_ref == mock.sentinel.N_ref
            assert self.s._dt == mock.sentinel.dt
            mock_order.assert_called_once_with(mock.sentinel.N_ref)
            mock_dt.assert_called_once_with(mock.sentinel.N)

            # verify that other args aren't affected
            assert self.s._timer is None
            assert not self.s._loggingEnabled

    def test_stop(self):
        self.s._isSubstep = True
        self.s._startTime = mock.sentinel.startTime
        self.s._N_ref = mock.sentinel.N_ref
        self.s._dt = mock.sentinel.dt
        mockTimer = mock.Mock(spec=threading.Timer)
        self.s._timer = mockTimer

        self.s.stop()

        # clears `_isSubstep`, `_startTime`, `_N_ref`, `_dt` and `_timer`:
        assert self.s._isSubstep == False  # noqa: E712
        assert self.s._startTime is None
        assert self.s._N_ref is None
        assert self.s._dt is None
        assert self.s._timer is None

        # cancels the `_timer`
        mockTimer.cancel.assert_called_once()

    def test_stop_no_timer(self):
        self.s._isSubstep = True
        self.s._startTime = mock.sentinel.startTime
        self.s._N_ref = mock.sentinel.N_ref
        self.s._dt = mock.sentinel.dt
        assert self.s._timer is None

        self.s.stop()

        # clears `_isSubstep`, `_startTime`, `_N_ref`, `_dt` and `_timer`:
        assert self.s._isSubstep == False  # noqa: E712
        assert self.s._startTime is None
        assert self.s._N_ref is None
        assert self.s._dt is None
        assert self.s._timer is None

    def test_stop_not_started(self):
        # checks that the step has been started
        assert self.s._startTime is None
        with pytest.raises(RuntimeError, match="Usage error: attempt to `stop` unstarted step.*"):
            self.s.stop()

    def test_recordMeasurement(self):
        # records the measurement
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            Config_override("application.workflows_data.timing.max_measurements", 500),
            Config_override("application.workflows_data.timing.update_minimum_count", 5),
            Config_override("application.workflows_data.timing.update_threshold", 0.2),
        ):
            _step = self.s.model_copy(deep=True)
            _step.estimate = mock.Mock()

            currentLength = len(_step.measurements)
            assert currentLength + 1 < Config["application.workflows_data.timing.update_minimum_count"]
            _step.recordMeasurement(dt_elapsed=100.0, dt_est=2 * 100.0, N_ref=4.0)
            assert len(_step.measurements) == currentLength + 1
            _step.estimate.update.assert_not_called()

        # updates the estimate when enough measurements are available
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            Config_override("application.workflows_data.timing.max_measurements", 500),
            Config_override("application.workflows_data.timing.update_minimum_count", 3),
            Config_override("application.workflows_data.timing.update_threshold", 0.2),
        ):
            _step = self.s.model_copy(deep=True)
            _step.details = mock.Mock()
            _step.estimate = mock.Mock()

            assert len(_step.measurements) >= Config["application.workflows_data.timing.update_minimum_count"]
            _step.recordMeasurement(dt_elapsed=100.0, dt_est=2 * 100.0, N_ref=4.0)
            assert len(_step.measurements) == 4

            # only the cached properties `step.N_ref`, `step.dt` should be used in `update`
            _step.details.N_ref.assert_not_called()
            _step.details.order.assert_not_called()
            _step.estimate.dt.assert_not_called()
            _step.estimate.update.assert_called_once_with(_step.measurements, _step.details.order)

        # only updates the estimate when the current estimate is bad
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            Config_override("application.workflows_data.timing.max_measurements", 500),
            Config_override("application.workflows_data.timing.update_minimum_count", 3),
            Config_override("application.workflows_data.timing.update_threshold", 0.2),
        ):
            _step = self.s.model_copy(deep=True)
            _step.estimate = mock.Mock()

            assert len(_step.measurements) >= Config["application.workflows_data.timing.update_minimum_count"]
            # record a measurement that has a _perfect_ estimate
            _step.recordMeasurement(dt_elapsed=100.0, dt_est=100.0, N_ref=4.0)
            assert len(_step.measurements) == 4
            _step.estimate.update.assert_not_called()

        # restricts maximum length of the `measurements` list
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            Config_override("application.workflows_data.timing.max_measurements", 25),
            Config_override("application.workflows_data.timing.update_minimum_count", 500),
            Config_override("application.workflows_data.timing.update_threshold", 0.2),
        ):
            _step = self.s.model_copy(deep=True)
            _step.measurements.extend([_step.measurements[-1] for n in range(25)])
            assert len(_step.measurements) > Config["application.workflows_data.timing.max_measurements"]
            _step.recordMeasurement(dt_elapsed=100.0, dt_est=100.0, N_ref=4.0)
            assert len(_step.measurements) == Config["application.workflows_data.timing.max_measurements"]

    def test_name(self):
        # calls `humanReadableName`
        with mock.patch.object(
            ProgressStep, "humanReadableName", return_value="module.class.func"
        ) as mockHumanReadableName:
            name = self.s.name
            assert isinstance(name, str)
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

    def test_isActive(self):
        _now = datetime.now(timezone.utc)

        # returns False if step hasn't been started
        assert self.s._startTime is None
        assert not self.s.isActive

        # returns True if step has been started
        self.s._startTime = _now
        assert self.s.isActive

    def test_startTime(self):
        _now = datetime.now(timezone.utc)

        # exception if the step hasn't been started
        with pytest.raises(RuntimeError, match="Usage error: attempt to read `startTime` for unstarted step.*"):
            _time = self.s.startTime

        # returns `_startTime`
        self.s._startTime = _now
        assert self.s.startTime == _now

    def test_N_ref(self):
        _now = datetime.now(timezone.utc)

        # exception if the step hasn't been started
        with pytest.raises(RuntimeError, match="Usage error: attempt to read `N_ref` for unstarted step.*"):
            _N_ref = self.s.N_ref

        # returns `_N_ref`
        self.s._startTime = _now
        self.s._N_ref = mock.sentinel.N_ref
        assert self.s.N_ref == mock.sentinel.N_ref

    def test_dt(self):
        _now = datetime.now(timezone.utc)

        # exception if the step hasn't been started
        with pytest.raises(RuntimeError, match="Usage error: attempt to read `dt` for unstarted step.*"):
            _dt = self.s.dt

        # returns `_N_ref`
        self.s._startTime = _now
        self.s._dt = mock.sentinel.dt
        assert self.s.dt == mock.sentinel.dt

    def test_loggingEnabled(self):
        _now = datetime.now(timezone.utc)

        # returns _loggingEnabled
        assert not self.s._loggingEnabled
        assert not self.s.loggingEnabled

        self.s._loggingEnabled = True
        assert self.s.loggingEnabled

    def test_isSubstep(self):
        # returns _isSubstep
        assert not self.s._isSubstep
        assert not self.s.isSubstep

        self.s._isSubstep = True
        assert self.s.isSubstep


# a function in module scope
def _function_in_module_scope():
    pass


class Test_ProgressRecorder:
    # pytest-style: uses fixtures

    @pytest.fixture(autouse=True, scope="class")
    @classmethod
    def _setup_test_class(cls):
        # setup
        yield

        # teardown
        pass

    @pytest.fixture(autouse=True)
    def _setup_test(self, Config_override_fixture):
        # setup

        """
        # The `ProgressRecorder` singleton has been created at import of its module.
        #   Since it is by-default _disabled_ in the test environment, it contains
        #   no persistent data; however, for all of these tests it now needs to be enabled:
        Config_override_fixture("application.workflows_data.timing.enable", True)
        """

        # Use a temporary directory for "user.application.data.home":
        Config_override_fixture(
            "user.application.data.home",
            tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/user.application.data.home_")),
        )
        yield

        # teardown

        # Re-initialize the `ProgressRecorder` at the end of each test.
        ProgressRecorder.steps.clear()

    @pytest.fixture
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

    @pytest.fixture(autouse=True)
    def _clear_instance_cache(self):
        # This fixture re-initializes `_ProgressRecorder.instance` `lru_cache` before and after each test.

        # setup
        _ProgressRecorder.instance.cache_clear()
        yield

        # teardown:
        _ProgressRecorder.instance.cache_clear()

    def test_init(self):
        # default init:
        recorder = _ProgressRecorder()
        assert recorder.steps == {}

    def test_enabled(self):
        # `enabled` classproperty tracks `Config`
        for flag in (False, True):
            with Config_override("application.workflows_data.timing.enabled", flag):
                assert _ProgressRecorder.enabled == flag

    def test_instance(self):
        # `_ProgressRecorder.instance()` uses `lru_cache`:
        #   for each of these subtests, we need to re-initialize it.

        # enabled => load persistent data
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            Config_override("application.workflows_data.timing.persistent_data", True),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "LocalDataService") as mockLocalDataService,
            mock.patch.object(_ProgressRecorder, "model_validate_json") as mockModelValidate,
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "atexit") as mock_atexit,
        ):
            _ProgressRecorder.instance.cache_clear()
            mockLocalDataService.return_value.readProgressRecords.return_value = mock.sentinel.json_data
            mockModelValidate.return_value = mock.sentinel.instance1
            actual = _ProgressRecorder.instance()
            assert actual == mock.sentinel.instance1
            mockModelValidate.assert_called_once_with(mock.sentinel.json_data)
            mockLocalDataService.return_value.readProgressRecords.assert_called_once()
            mock_atexit.register.assert_called_once_with(_ProgressRecorder._unloadResident)

            # call it again => returns the cached value
            mockLocalDataService.reset_mock()
            mockModelValidate.reset_mock()
            mock_atexit.reset_mock()
            mockModelValidate.return_value = mock.sentinel.instance2
            actual = _ProgressRecorder.instance()
            assert actual == mock.sentinel.instance1
            mockModelValidate.assert_not_called()
            mockLocalDataService.return_value.readProgressRecords.assert_not_called()
            mock_atexit.register.assert_not_called()

        # enabled (not 'persistent_data') => load persistent data, but don't register to unload it
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            Config_override("application.workflows_data.timing.persistent_data", False),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "LocalDataService") as mockLocalDataService,
            mock.patch.object(_ProgressRecorder, "model_validate_json") as mockModelValidate,
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "atexit") as mock_atexit,
        ):
            _ProgressRecorder.instance.cache_clear()
            mockLocalDataService.return_value.readProgressRecords.return_value = mock.sentinel.json_data
            mockModelValidate.return_value = mock.sentinel.instance1
            actual = _ProgressRecorder.instance()
            assert actual == mock.sentinel.instance1
            mockModelValidate.assert_called_once_with(mock.sentinel.json_data)
            mockLocalDataService.return_value.readProgressRecords.assert_called_once()
            mock_atexit.register.assert_not_called()

        # disabled => do not load persistent data
        with (
            Config_override("application.workflows_data.timing.enabled", False),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "LocalDataService") as mockLocalDataService,
            mock.patch.object(_ProgressRecorder, "model_validate_json") as mockModelValidate,
            mock.patch.object(_ProgressRecorder, "__new__") as mock_new,
            mock.patch.object(_ProgressRecorder, "__init__") as mock_init,
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "atexit") as mock_atexit,
        ):
            _ProgressRecorder.instance.cache_clear()
            mockLocalDataService.return_value.readProgressRecords.side_effect = RuntimeError(
                "this method should not be called"
            )
            mockModelValidate.side_effect = RuntimeError("this method should not be called")
            mock_atexit.register.side_effect = RuntimeError("this function should not be called")
            mock_new.return_value = mock.sentinel.instance3
            actual = _ProgressRecorder.instance()
            assert actual == mock.sentinel.instance3
            mock_new.assert_called_once_with(_ProgressRecorder)

            # call it again => returns the cached value
            mockLocalDataService.reset_mock()
            mockModelValidate.reset_mock()
            mock_atexit.reset_mock()
            mock_init.reset_mock()
            mock_new.reset_mock()
            mock_new.return_value = mock.sentinel.instance4
            actual = _ProgressRecorder.instance()
            assert actual == mock.sentinel.instance3
            mock_new.assert_not_called()

    def test__unloadResident(self):
        # `_ProgressRecorder.instance()` uses `lru_cache`:
        #   for each of these subtests, we need to re-initialize it.

        # enabled: saves persistent data
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "LocalDataService") as mockLocalDataService,
            mock.patch.object(_ProgressRecorder, "instance") as mockInstance,
        ):
            _ProgressRecorder.instance.cache_clear()
            mockInstance.return_value.model_dump_json.return_value = mock.sentinel.json_data
            _ProgressRecorder._unloadResident()
            mockInstance.assert_called_once()
            mockLocalDataService.return_value.writeProgressRecords.assert_called_once_with(mock.sentinel.json_data)

        # disabled: does not save any data
        with (
            Config_override("application.workflows_data.timing.enabled", False),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "LocalDataService") as mockLocalDataService,
            mock.patch.object(_ProgressRecorder, "instance") as mockInstance,
        ):
            _ProgressRecorder.instance.cache_clear()
            mockInstance.return_value.model_dump_json.return_value = mock.sentinel.json_data
            _ProgressRecorder._unloadResident()
            mockInstance.return_value.model_dump_json.assert_not_called()
            mockLocalDataService.return_value.writeProgressRecords.assert_not_called()

        # enabled: exceptions raised at exit do not propagate
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "LocalDataService") as mockLocalDataService,
            mock.patch.object(_ProgressRecorder, "instance") as mockInstance,
        ):
            mockLocalDataService.return_value.writeProgressRecords.side_effect = RuntimeError("any exception")
            _ProgressRecorder.instance.cache_clear()
            mockInstance.return_value.model_dump_json.return_value = mock.sentinel.json_data
            _ProgressRecorder._unloadResident()
            mockInstance.assert_called_once()
            mockLocalDataService.return_value.writeProgressRecords.assert_called_once_with(mock.sentinel.json_data)

    def test__getCallerFullyQualifiedName(self):
        # function in local scope
        def _function():
            pass

        # object of type `type`
        class _Class:
            def method(self):
                pass

        # object of type `_Class`
        instanceType = _Class()

        # `FunctionType` <- `LambdaType` in local scope
        lambdaType = lambda *_args, **_kwargs: None  # noqa: E731

        # function in local scope
        expected = (__name__, "Test_ProgressRecorder.test__getCallerFullyQualifiedName.<locals>._function")
        actual = _ProgressRecorder._getCallerFullyQualifiedName(_function)
        assert actual == expected

        # function in module scope
        expected = (__name__, "_function_in_module_scope")
        actual = _ProgressRecorder._getCallerFullyQualifiedName(_function_in_module_scope)
        assert actual == expected

        # unbound class method
        expected = (__name__, "Test_ProgressRecorder.test__getCallerFullyQualifiedName")
        actual = _ProgressRecorder._getCallerFullyQualifiedName(Test_ProgressRecorder.test__getCallerFullyQualifiedName)
        assert actual == expected

        # lambda function
        #   (Note: generally this would not be used, but it _is_ allowed.)
        expected = (__name__, "Test_ProgressRecorder.test__getCallerFullyQualifiedName.<locals>.<lambda>")
        actual = _ProgressRecorder._getCallerFullyQualifiedName(lambda *_args, **_kwargs: None)
        assert actual == expected

        # reference to a lambda function
        expected = (__name__, "Test_ProgressRecorder.test__getCallerFullyQualifiedName.<locals>.<lambda>")
        actual = _ProgressRecorder._getCallerFullyQualifiedName(lambdaType)
        assert actual == expected

        # a class
        expected = (__name__, "Test_ProgressRecorder.test__getCallerFullyQualifiedName.<locals>._Class")
        actual = _ProgressRecorder._getCallerFullyQualifiedName(_Class)
        assert actual == expected

        # the current stack frame
        expected = (__name__, "Test_ProgressRecorder.test__getCallerFullyQualifiedName")
        actual = _ProgressRecorder._getCallerFullyQualifiedName(inspect.currentframe())
        assert actual == expected

        # one stack frame up from the current frame
        expected = ("_pytest.python", "pytest_pyfunc_call")
        actual = _ProgressRecorder._getCallerFullyQualifiedName(inspect.currentframe().f_back)
        assert actual == expected

        # anything else should raise an exception
        for caller in (instanceType, instanceType.method, "string", None):
            with pytest.raises(
                RuntimeError, match=".*Expecting object of type `FrameType`, `FunctionType`, or `type` only.*"
            ):
                _ProgressRecorder._getCallerFullyQualifiedName(caller)

    def test__getCallerFullyQualifiedName_python_version(self):
        # function in local scope
        def _function():
            pass

        # object of type `type`
        class _Class:
            def method(self):
                pass

        # object of type `_Class`
        _instanceType = _Class()

        # `FunctionType` <- `LambdaType` in local scope
        _lambdaType = lambda *_args, **_kwargs: None  # noqa: E731

        VersionInfo = namedtuple("VersionInfo", ["major", "minor", "micro", "releaselevel", "serial"])
        with mock.patch.object(sys, "version_info", VersionInfo(3, 10, 0, "final", 0)):
            # the current stack frame
            expected = (__name__, "Test_ProgressRecorder.test__getCallerFullyQualifiedName_python_version")
            actual = _ProgressRecorder._getCallerFullyQualifiedName(inspect.currentframe())
            assert actual == expected

            # one stack frame up from the current frame
            expected = ("_pytest.python", "pytest_pyfunc_call")
            actual = _ProgressRecorder._getCallerFullyQualifiedName(inspect.currentframe().f_back)
            assert actual == expected

    def test_isLoggingEnabledForStep(self):
        test_qualname_roots = ["joe", "sam", "sally"]
        _ProgressRecorder._loggable_qualname_roots.cache_clear()
        with Config_override("application.workflows_data.timing.logging.qualname_roots", test_qualname_roots):
            # key ::= (<module name> | None, <fully-qualified function name> | None, <step name> | None)

            # trivial key returns False
            assert not _ProgressRecorder.isLoggingEnabledForStep((None, None, None))

            # key with matching <qualified name> returns True
            assert _ProgressRecorder.isLoggingEnabledForStep(("module", "joe.blah.blah", "step-name"))

            # key with non-matching <qualified name> returns False
            assert not _ProgressRecorder.isLoggingEnabledForStep(("module", "ted.blah.blah", "step-name"))

            # key with None elements and match returns True
            assert _ProgressRecorder.isLoggingEnabledForStep((None, "sam.blah.blah", None))

            # key with None elements and non-matching <qualified name> returns False
            assert not _ProgressRecorder.isLoggingEnabledForStep((None, "fred.blah.blah", None))

    def test_getStepKey(self):
        # object of type `type`
        class _Class:
            pass

        # object of type `FunctionType`
        def _function():
            pass

        # object of type `FrameType`:
        #   the caller of this test method's stack frame
        default_frame = inspect.currentframe().f_back

        # returns result from `_getCallerFullyQualifiedName` + (<step name>,)
        with mock.patch.object(_ProgressRecorder, "_getCallerFullyQualifiedName") as mock_getFullyQualifiedName:
            stepName = "the-step"
            qualified_name = ("module.name", "qualified.function.name")
            mock_getFullyQualifiedName.return_value = qualified_name
            expected = qualified_name + (stepName,)

            # works with `FunctionType`
            actual = _ProgressRecorder.getStepKey(callerOrStackFrameOverride=_function, stepName=stepName)
            assert actual == expected
            mock_getFullyQualifiedName.assert_called_once_with(_function)

            # works with `FrameType`
            mock_getFullyQualifiedName.reset_mock()
            actual = _ProgressRecorder.getStepKey(callerOrStackFrameOverride=default_frame, stepName=stepName)
            assert actual == expected
            mock_getFullyQualifiedName.assert_called_once_with(default_frame)

            # no <step name> adds `None` in the <step name> position
            mock_getFullyQualifiedName.reset_mock()
            expected = qualified_name + (None,)
            actual = _ProgressRecorder.getStepKey(callerOrStackFrameOverride=_function)
            assert actual == expected
            mock_getFullyQualifiedName.assert_called_once_with(_function)

        # only allows `FunctionType`, `FrameType`, or `type`
        with mock.patch.object(_ProgressRecorder, "_getCallerFullyQualifiedName") as mock_getFullyQualifiedName:
            with pytest.raises(  # noqa: PT012
                RuntimeError,
                match=".*it must be either a function, the local stack-frame of a function, or a <class> type.*",
            ):
                _class = _Class()  # an instance of `_Class`
                _ProgressRecorder.getStepKey(callerOrStackFrameOverride=_class)

        # By default `_getStepKey` uses the stack frame _one_ frame up from the current frame:
        #   this allows `_ProgressRecorder.record` to use `_getStepKey` and obtain the step for
        #     the scope of its _caller_.
        with mock.patch.object(_ProgressRecorder, "_getCallerFullyQualifiedName") as mock_getFullyQualifiedName:
            stepName = "the-step"
            qualified_name = ("module.name", "qualified.function.name")
            mock_getFullyQualifiedName.return_value = qualified_name
            expected = qualified_name + (stepName,)
            actual = _ProgressRecorder.getStepKey(stepName=stepName)
            assert actual == expected
            mock_getFullyQualifiedName.assert_called_once_with(default_frame)

    def test_getStep(self):
        instance = _ProgressRecorder()
        key = (__name__, "test_ProgressRecorder.test_getStep", None)
        step = ProgressStep(details=_Step(key))

        # step is in <instance>.steps
        assert instance.steps == {}
        with mock.patch.dict(instance.steps, {key: step}, clear=True):
            assert key in instance.steps
            assert instance.getStep(key, create=False) == step

        # step not in <instance>.steps: create=False
        with pytest.raises(  # noqa: PT012
            RuntimeError, match=re.escape(f"Usage error: progress-recording step {key} does not exist.")
        ):
            assert key not in instance.steps
            instance.getStep(key, create=False)

        # step not in <instance>.steps, create=True =>
        #   creates the step when it doesn't exist,
        #   fills in its default values,
        #   and enters it in `steps`
        with mock.patch.dict(instance.steps, {}, clear=True):
            assert key not in instance.steps
            newStep = instance.getStep(key, create=True)
            assert instance.steps[key] == newStep
            # they have the same key, but they should be distinct objects
            assert id(newStep) != id(step)
            # they should have the same attributes
            assert newStep == step

    def test_logTimeRemaining(self):
        key = (__name__, "test_ProgressRecorder.test_logTimeRemaining", None)
        step = ProgressStep(details=_Step(key))
        instance = _ProgressRecorder(steps={key: step})

        # calls `getStep`, `_logTimeRemaining`
        with (
            mock.patch.object(_ProgressRecorder, "getStep", return_value=step) as mockGetStep,
            mock.patch.object(_ProgressRecorder, "_logTimeRemaining") as mock_logTimeRemaining,
        ):
            instance.logTimeRemaining(key)
            mockGetStep.assert_called_once_with(key)
            mock_logTimeRemaining.assert_called_once_with(step)

    def test__chainLogTimeRemaining(self):
        update_interval = 10.0
        instance = _ProgressRecorder()
        key = (__name__, "test_ProgressRecorder.test__chainLogTimeRemaining", None)
        substepKey = (__name__, "test_ProgressRecorder.test__chainLogTimeRemaining", "a-substep")

        # calls `_logTimeRemaining`
        with mock.patch.object(_ProgressRecorder, "_logTimeRemaining") as mock_logTimeRemaining:
            mock_logTimeRemaining.return_value = False
            step = ProgressStep(details=_Step(key))
            substep = ProgressStep(details=_Step(substepKey))
            substep._isSubstep = True
            instance = _ProgressRecorder(steps={key: step, substepKey: substep})

            assert step._timer is None
            instance._chainLogTimeRemaining(step)
            mock_logTimeRemaining.assert_called_once_with(step)

        # `_logTimeRemaining` returns True:
        #    'log_update_interval' > 0.0: set a `Timer`
        with (
            Config_override("application.workflows_data.timing.logging.log_update_interval", update_interval),
            mock.patch.object(_ProgressRecorder, "_logTimeRemaining") as mock_logTimeRemaining,
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "Timer") as mockTimer,
        ):
            mock_logTimeRemaining.return_value = True
            step = ProgressStep(details=_Step(key))
            substep = ProgressStep(details=_Step(substepKey))
            substep._isSubstep = True
            instance = _ProgressRecorder(steps={key: step, substepKey: substep})

            assert step._timer is None
            assert instance._logTimeRemaining()
            instance._logTimeRemaining.reset_mock()
            instance._chainLogTimeRemaining(step)
            assert step._timer is not None
            mockTimer.assert_called_once_with(update_interval, instance._chainLogTimeRemaining, args=[step])

        # `_logTimeRemaining` returns True:
        #    'log_update_interval' == 0.0: do not set a `Timer`
        with (
            Config_override("application.workflows_data.timing.logging.log_update_interval", 0.0),
            mock.patch.object(_ProgressRecorder, "_logTimeRemaining") as mock_logTimeRemaining,
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "Timer") as mockTimer,
        ):
            mock_logTimeRemaining.return_value = True
            step = ProgressStep(details=_Step(key))
            substep = ProgressStep(details=_Step(substepKey))
            substep._isSubstep = True
            instance = _ProgressRecorder(steps={key: step, substepKey: substep})

            assert step._timer is None
            assert instance._logTimeRemaining()
            instance._logTimeRemaining.reset_mock()
            instance._chainLogTimeRemaining(step)
            assert step._timer is None
            mockTimer.assert_not_called()

        # `_logTimeRemaining` returns True:
        #    'log_update_interval' > 0.0: is a substep: do not set a `Timer`
        with (
            Config_override("application.workflows_data.timing.logging.log_update_interval", update_interval),
            mock.patch.object(_ProgressRecorder, "_logTimeRemaining") as mock_logTimeRemaining,
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "Timer") as mockTimer,
        ):
            mock_logTimeRemaining.return_value = True
            step = ProgressStep(details=_Step(key))
            substep = ProgressStep(details=_Step(substepKey))
            substep._isSubstep = True
            instance = _ProgressRecorder(steps={key: step, substepKey: substep})

            assert substep._timer is None
            assert instance._logTimeRemaining()
            instance._logTimeRemaining.reset_mock()
            instance._chainLogTimeRemaining(substep)
            assert substep._timer is None
            mockTimer.assert_not_called()

        # `_logTimeRemaining` returns False:
        #    if `step._timer is not None`: `cancel` and set to `None`
        with (
            Config_override("application.workflows_data.timing.logging.log_update_interval", update_interval),
            mock.patch.object(_ProgressRecorder, "_logTimeRemaining") as mock_logTimeRemaining,
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "Timer") as mockTimer,
        ):
            mock_logTimeRemaining.return_value = False
            step = ProgressStep(details=_Step(key))
            substep = ProgressStep(details=_Step(substepKey))
            substep._isSubstep = True
            instance = _ProgressRecorder(steps={key: step, substepKey: substep})
            mock_step__timer = mock.Mock()
            step._timer = mock_step__timer

            assert not instance._logTimeRemaining()
            instance._logTimeRemaining.reset_mock()
            instance._chainLogTimeRemaining(step)
            # the `step._timer` has been cancelled and set to `None`
            assert step._timer is None
            mock_step__timer.cancel.assert_called_once()
            mockTimer.assert_not_called()

    def test__logTimeRemaining(self, caplog):
        instance = _ProgressRecorder()
        key = (__name__, "test_ProgressRecorder.test__logTimeRemaining", None)
        substepKey = (__name__, "test_ProgressRecorder.test__logTimeRemaining", "a-substep")
        step = ProgressStep(details=_Step(key))
        substep = ProgressStep(details=_Step(substepKey))
        substep._isSubstep = True

        # for active step:
        #   calls `_loggableStepName`, `_stepTimeRemaining`
        with (
            mock.patch.object(step, "_isActive", return_value=True) as mock_isActive,
            mock.patch.dict(instance.steps, {key: step, substepKey: substep}, clear=True),
            mock.patch.object(_ProgressRecorder, "_loggableStepName") as mock_loggableStepName,
            mock.patch.object(_ProgressRecorder, "_stepTimeRemaining") as mock_stepTimeRemaining,
        ):
            remainder = 10.0
            mock_loggableStepName.return_value = "test__logTimeRemaining"
            mock_stepTimeRemaining.return_value = remainder

            assert step.isActive
            mock_isActive.reset_mock()
            assert instance._stepTimeRemaining(step) == remainder
            instance._stepTimeRemaining.reset_mock()

            instance._logTimeRemaining(step)
            mock_loggableStepName.assert_called_once_with(step.details.key, False)
            mock_stepTimeRemaining.assert_called_once_with(step)

        # for inactive step: does nothing: returns False
        with (
            mock.patch.object(step, "_isActive", return_value=False) as mock_isActive,
            mock.patch.dict(instance.steps, {key: step, substepKey: substep}, clear=True),
            mock.patch.object(_ProgressRecorder, "_loggableStepName") as mock_loggableStepName,
            mock.patch.object(_ProgressRecorder, "_stepTimeRemaining") as mock_stepTimeRemaining,
        ):
            remainder = 10.0
            mock_loggableStepName.return_value = "test__logTimeRemaining"
            mock_stepTimeRemaining.return_value = remainder

            assert not step.isActive
            mock_isActive.reset_mock()
            continueToLog = instance._logTimeRemaining(step)
            mock_loggableStepName.assert_not_called()
            mock_stepTimeRemaining.assert_not_called()
            assert not continueToLog

        # for active step:
        #   remainder is `None`: log "<no timing data available>": returns False
        with (
            mock.patch.object(step, "_isActive", return_value=True) as mock_isActive,
            mock.patch.dict(instance.steps, {key: step, substepKey: substep}, clear=True),
            mock.patch.object(_ProgressRecorder, "_loggableStepName") as mock_loggableStepName,
            mock.patch.object(_ProgressRecorder, "_stepTimeRemaining") as mock_stepTimeRemaining,
        ):
            remainder = None
            mock_loggableStepName.return_value = "test__logTimeRemaining"
            mock_stepTimeRemaining.return_value = remainder

            assert step.isActive
            mock_isActive.reset_mock()
            assert instance._stepTimeRemaining(step) == remainder
            mock_stepTimeRemaining.reset_mock()
            with caplog.at_level(
                Config["application.workflows_data.timing.logging.loglevel"],
                logger=inspect.getmodule(ProgressRecorder).__name__,
            ):
                continueToLog = instance._logTimeRemaining(step)
                assert not continueToLog
            assert "<no timing data is available>" in caplog.text
            assert mock_loggableStepName.return_value in caplog.text
        caplog.clear()

        # for active step:
        #   remainder > 0.0: log time-remaning message: returns True
        with (
            mock.patch.object(step, "_isActive", return_value=True) as mock_isActive,
            mock.patch.dict(instance.steps, {key: step, substepKey: substep}, clear=True),
            mock.patch.object(_ProgressRecorder, "_loggableStepName") as mock_loggableStepName,
            mock.patch.object(_ProgressRecorder, "_stepTimeRemaining") as mock_stepTimeRemaining,
        ):
            remainder = 10.0
            mock_loggableStepName.return_value = "test__logTimeRemaining"
            mock_stepTimeRemaining.return_value = remainder

            assert step.isActive
            mock_isActive.reset_mock()
            assert instance._stepTimeRemaining(step) == remainder
            mock_stepTimeRemaining.reset_mock()
            with caplog.at_level(
                Config["application.workflows_data.timing.logging.loglevel"],
                logger=inspect.getmodule(ProgressRecorder).__name__,
            ):
                continueToLog = instance._logTimeRemaining(step)
                assert continueToLog
            assert f"estimated completion in {remainder} seconds" in caplog.text
            assert mock_loggableStepName.return_value in caplog.text
        caplog.clear()

        # for active step:
        #   remainder == 0.0: log "is taking longer than expected" message: returns False
        with (
            mock.patch.object(step, "_isActive", return_value=True) as mock_isActive,
            mock.patch.dict(instance.steps, {key: step, substepKey: substep}, clear=True),
            mock.patch.object(instance, "_loggableStepName") as mock_loggableStepName,
            mock.patch.object(instance, "_stepTimeRemaining") as mock_stepTimeRemaining,
        ):
            remainder = 0.0
            mock_loggableStepName.return_value = "test__logTimeRemaining"
            mock_stepTimeRemaining.return_value = remainder

            assert step.isActive
            mock_isActive.reset_mock()
            assert instance._stepTimeRemaining(step) == remainder
            mock_stepTimeRemaining.reset_mock()
            with caplog.at_level(
                Config["application.workflows_data.timing.logging.loglevel"],
                logger=inspect.getmodule(ProgressRecorder).__name__,
            ):
                continueToLog = instance._logTimeRemaining(step)
                assert not continueToLog
            assert "taking longer than expected" in caplog.text
            assert mock_loggableStepName.return_value in caplog.text
        caplog.clear()

    def test__logCompletion(self, caplog):
        instance = _ProgressRecorder()
        key = (__name__, "test_ProgressRecorder.test__logCompletion", None)
        substepKey = (__name__, "test_ProgressRecorder.test__logCompletion", "a-substep")
        step = ProgressStep(details=_Step(key))
        substep = ProgressStep(details=_Step(substepKey))
        substep._isSubstep = True
        with (
            mock.patch.dict(instance.steps, {key: step, substepKey: substep}, clear=True),
            mock.patch.object(instance, "_loggableStepName", wraps=instance._loggableStepName) as mock_loggableStepName,
        ):
            # calls `_loggableStepName`
            instance._logCompletion(step.details.key, False)
            mock_loggableStepName.assert_called_once_with(step.details.key, False)

        with mock.patch.dict(instance.steps, {key: step, substepKey: substep}, clear=True):
            # logs at the level of `Config["application.workflows_data.timing.logging.loglevel"]`
            expectedName = instance._loggableStepName(step.details.key, False)
            with caplog.at_level(
                Config["application.workflows_data.timing.logging.loglevel"],
                logger=inspect.getmodule(ProgressRecorder).__name__,
            ):
                instance._logCompletion(step.details.key, False)
            # logs a completion message
            assert "complete" in caplog.text.lower()
            # includes the step name
            assert expectedName in caplog.text

            caplog.clear()
            expectedName = instance._loggableStepName(substep.details.key, True)
            with caplog.at_level(
                Config["application.workflows_data.timing.logging.loglevel"],
                logger=inspect.getmodule(ProgressRecorder).__name__,
            ):
                instance._logCompletion(substep.details.key, True)
            assert "complete" in caplog.text.lower()
            # or includes the <short name>, as appropriate.
            assert expectedName in caplog.text

    def test__loggableStepName(self):
        instance = _ProgressRecorder()
        key = (__name__, "test_ProgressRecorder.test__loggableStepName", "main-step")
        substepKey = (__name__, "test_ProgressRecorder.test__loggableStepName", "a-substep")
        step = ProgressStep(details=_Step(key))
        substep = ProgressStep(details=_Step(substepKey))
        substep._isSubstep = True

        with mock.patch.dict(instance.steps, {key: step, substepKey: substep}, clear=True):
            # the step is not a substep => use its full name
            assert instance._loggableStepName(step.details.key, False) == "test__loggableStepName: main-step"

            # the step is a substep => use a shorter name
            shortName = instance._loggableStepName(substep.details.key, True)
            #   the <short name> is actually shorter
            assert len(shortName) < len(step.name)
            #   the substep's name is in the <short name>
            assert substepKey[-1] in shortName

    def test_stepTimeRemaining(self):
        key = (__name__, "test_ProgressRecorder.test_stepTimeRemaining", None)
        step = ProgressStep(details=_Step(key))

        # calls `getStep`, `_stepTimeRemaining`: returns `_stepTimeRemaining`
        with (
            mock.patch.object(_ProgressRecorder, "getStep", return_value=step) as mockGetStep,
            mock.patch.object(
                _ProgressRecorder, "_stepTimeRemaining", return_value=mock.sentinel.remainder
            ) as mock_stepTimeRemaining,
        ):
            instance = _ProgressRecorder(steps={key: step})
            assert instance.stepTimeRemaining(key) == mock.sentinel.remainder
            mockGetStep.assert_called_once_with(key)
            mock_stepTimeRemaining.assert_called_once_with(step)

    def test__stepTimeRemaining(self):
        _now = datetime.now(timezone.utc)
        _start = _now - timedelta(seconds=120.0)
        _elapsed = (_now - _start).total_seconds()
        # estimated dt > elapsed time:
        _dt_continues = 180.0
        # estimated dt < elapsed time:
        _dt_finished = 60.0
        key = (__name__, "test_ProgressRecorder.test__stepTimeRemaining", None)

        # does NOT call `step.N_ref`, `step.order`, `step.estimate.dt`: returns the `step.dt` value
        with (
            mock.patch.object(_Step, "N_ref", return_value=mock.sentinel.N_ref) as mock_N_ref,
            mock.patch.object(_Estimate, "dt", return_value=mock.sentinel.dt) as mock_dt,
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "datetime") as mock_datetime,
        ):
            mock_datetime.now = mock.Mock(return_value=_now)
            step = ProgressStep(details=_Step(key))
            step._startTime = _start
            step._N_ref = mock.sentinel.N_ref
            step._dt = _dt_continues
            mock_order = mock.Mock(return_value=mock.sentinel.N)
            step.details.order = mock_order

            expected = _dt_continues - _elapsed
            actual = _ProgressRecorder._stepTimeRemaining(step)
            assert actual == pytest.approx(expected, rel=1.0e-3)
            mock_N_ref.assert_not_called()
            mock_order.assert_not_called()
            mock_dt.assert_not_called()

        # remaining time < 0.0: returns 0.0
        with (
            mock.patch.object(_Step, "N_ref", return_value=mock.sentinel.N_ref) as mock_N_ref,
            mock.patch.object(_Estimate, "dt", return_value=mock.sentinel.dt) as mock_dt,
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "datetime") as mock_datetime,
        ):
            mock_datetime.now = mock.Mock(return_value=_now)
            step = ProgressStep(details=_Step(key))
            step._startTime = _start
            step._N_ref = mock.sentinel.N_ref
            step._dt = _dt_finished
            mock_order = mock.Mock(return_value=mock.sentinel.N)
            step.details.order = mock_order

            expected = 0.0
            actual = _ProgressRecorder._stepTimeRemaining(step)
            assert actual == expected
            mock_N_ref.assert_not_called()
            mock_order.assert_not_called()
            mock_dt.assert_not_called()

        # `step.details.N_ref() is None`: returns `None`
        with (
            mock.patch.object(_Step, "N_ref", return_value=mock.sentinel.N_ref) as mock_N_ref,
            mock.patch.object(_Estimate, "dt", return_value=mock.sentinel.dt) as mock_dt,
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "datetime") as mock_datetime,
        ):
            mock_datetime.now = mock.Mock(return_value=_now)
            step = ProgressStep(details=_Step(key))
            step._startTime = _start
            step._N_ref = None
            step._dt = None
            mock_order = mock.Mock(return_value=mock.sentinel.N)
            step.details.order = mock_order

            expected = None
            actual = _ProgressRecorder._stepTimeRemaining(step)
            assert actual == expected
            mock_N_ref.assert_not_called()
            mock_order.assert_not_called()
            mock_dt.assert_not_called()

    def test__validate_steps(self):
        stepsList = [ProgressStep(details=_Step(key=(__name__, f"function_{n}", None))) for n in range(20)]
        stepsDict = {s.details.key: s for s in stepsList}

        # `Dict[Tuple[str, ...], <ProgressStep dict>]` <- `List[ProgressStep]`
        assert _ProgressRecorder._validate_steps(stepsList) == stepsDict

        # `Dict[Tuple[str, ...], ProgressStep]` remains unchanged
        assert _ProgressRecorder._validate_steps(stepsDict) == stepsDict

        # works as expected when called via `model_validate_json`
        jsonData = Resource.read("inputs/workflows_data/execution_timing_2025-07-20T03:46:43.913617+00:00.json")
        _ProgressRecorder.model_validate_json(jsonData)

    def test__serialize_steps(self):
        stepsList = [ProgressStep(details=_Step(key=(__name__, f"function_{n}", None))) for n in range(20)]
        stepsDict = {s.details.key: s for s in stepsList}
        instance = _ProgressRecorder(steps=stepsDict)

        # `List[ProgressStep]` <- `Dict[Tuple[str, ...], ProgressStep]`
        assert instance._serialize_steps(instance.steps, mock.sentinel._info) == stepsList

    def test_record(self):
        caller = mock.sentinel.caller
        N_ref = mock.sentinel.N_ref
        N_ref_args = (mock.sentinel.N_ref_args,)
        order = mock.sentinel.order
        stepName = mock.sentinel.stepName
        key0 = mock.sentinel.key0
        key1 = mock.sentinel.key1

        # enabled: gets (or creates) the step, sets its details, calls step start,
        #   starts the logging chain: returns the step key
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            mock.patch.object(_ProgressRecorder, "getStepKey") as mockGetStepKey,
            mock.patch.object(_ProgressRecorder, "getStep") as mockGetStep,
            mock.patch.object(_ProgressRecorder, "isLoggingEnabledForStep") as mockIsLoggingEnabledForStep,
            mock.patch.object(_ProgressRecorder, "_chainLogTimeRemaining") as mock_chainLogTimeRemaining,
        ):
            mockGetStepKey.return_value = key0
            step0 = mock.Mock(spec=ProgressStep, details=mock.Mock(spec=_Step, key=key0))
            mockGetStep.return_value = step0
            mockIsLoggingEnabledForStep.return_value = True

            instance = _ProgressRecorder.model_construct(steps={key0: step0})
            instance._activeSteps = []

            returnValue = instance.record(
                callerOrStackFrameOverride=caller, stepName=stepName, N_ref=N_ref, N_ref_args=N_ref_args, order=order
            )
            assert returnValue == key0
            assert instance._activeSteps == [step0]

            mockGetStepKey.assert_called_once_with(callerOrStackFrameOverride=caller, stepName=stepName)
            mockGetStep.assert_called_once_with(key0, create=True)
            mockIsLoggingEnabledForStep.assert_called_once_with(key0)
            step0.setDetails.assert_called_once_with(
                N_ref=N_ref, N_ref_args=N_ref_args, order=order, enableLogging=True
            )
            step0.start.assert_called_once_with(isSubstep=False)
            mock_chainLogTimeRemaining.assert_called_once_with(step0)

        # enabled (substep): gets (or creates) the step, sets its details, calls step start,
        #   starts the logging chain: returns the step key
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            mock.patch.object(_ProgressRecorder, "getStepKey") as mockGetStepKey,
            mock.patch.object(_ProgressRecorder, "getStep") as mockGetStep,
            mock.patch.object(_ProgressRecorder, "isLoggingEnabledForStep") as mockIsLoggingEnabledForStep,
            mock.patch.object(_ProgressRecorder, "_chainLogTimeRemaining") as mock_chainLogTimeRemaining,
        ):
            mockGetStepKey.return_value = key0
            step0 = mock.Mock(spec=ProgressStep, details=mock.Mock(spec=_Step, key=key0))
            step1 = mock.Mock(spec=ProgressStep, details=mock.Mock(spec=_Step, key=key1))
            mockGetStep.return_value = step0
            mockIsLoggingEnabledForStep.return_value = True

            instance = _ProgressRecorder.model_construct(steps={key0: step0, key1: step1})
            # a substep is identified when `_activeSteps` isn't empty
            instance._activeSteps = [step1]

            returnValue = instance.record(
                callerOrStackFrameOverride=caller, stepName=stepName, N_ref=N_ref, N_ref_args=N_ref_args, order=order
            )
            assert returnValue == key0
            assert instance._activeSteps == [step1, step0]

            mockGetStepKey.assert_called_once_with(callerOrStackFrameOverride=caller, stepName=stepName)
            mockGetStep.assert_called_once_with(key0, create=True)
            mockIsLoggingEnabledForStep.assert_called_once_with(key0)
            step0.setDetails.assert_called_once_with(
                N_ref=N_ref, N_ref_args=N_ref_args, order=order, enableLogging=True
            )
            step0.start.assert_called_once_with(isSubstep=True)
            mock_chainLogTimeRemaining.assert_called_once_with(step0)

        # disabled: returns None
        with (
            Config_override("application.workflows_data.timing.enabled", False),
            mock.patch.object(_ProgressRecorder, "getStepKey") as mockGetStepKey,
            mock.patch.object(_ProgressRecorder, "getStep") as mockGetStep,
            mock.patch.object(_ProgressRecorder, "isLoggingEnabledForStep") as mockIsLoggingEnabledForStep,
            mock.patch.object(_ProgressRecorder, "_chainLogTimeRemaining") as mock_chainLogTimeRemaining,
        ):
            mockGetStepKey.return_value = key0
            step0 = mock.Mock(spec=ProgressStep, details=mock.Mock(spec=_Step, key=key0))
            mockGetStep.return_value = step0
            mockIsLoggingEnabledForStep.return_value = True

            instance = _ProgressRecorder.model_construct(steps={key0: step0})
            returnValue = instance.record(
                callerOrStackFrameOverride=caller, stepName=stepName, N_ref=N_ref, N_ref_args=N_ref_args, order=order
            )
            assert returnValue is None

            mockGetStepKey.assert_not_called()
            mockGetStep.assert_not_called()
            mockIsLoggingEnabledForStep.assert_not_called()
            mock_chainLogTimeRemaining.assert_not_called()

    def test_stop(self):
        _now = datetime.now(timezone.utc)
        _start = _now - timedelta(seconds=120.0)
        _elapsed = (_now - _start).total_seconds()
        key0 = mock.sentinel.key0
        N_ref = 4096.0
        N = N_ref**2

        # enabled: gets the step, checks the stop time, calls `step.stop`,
        #   logs the completion
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            Config_override("application.workflows_data.timing.max_measurements", 50),
            Config_override("application.workflows_data.timing.update_minimum_count", 10),
            Config_override("application.workflows_data.timing.update_threshold", 0.2),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "datetime") as mock_datetime,
            mock.patch.object(inspect.getmodule(_ProgressRecorder).sys, "exc_info") as mock_exc_info,
            mock.patch.object(_ProgressRecorder, "getStep") as mockGetStep,
            mock.patch.object(_ProgressRecorder, "_logCompletion") as mock_logCompletion,
        ):
            mock_datetime.now.return_value = _now
            mock_exc_info.return_value = (None, None, None)
            step0 = mock.Mock(
                spec=ProgressStep,
                details=mock.Mock(
                    spec=_Step, key=key0, N_ref=mock.Mock(return_value=N_ref), order=mock.Mock(return_value=N)
                ),
                measurements=[],
                estimate=mock.Mock(spec=_Estimate),
                stop=mock.Mock(),
                recordMeasurement=mock.Mock(),
            )
            # mock the `_Step` properties
            mock_loggingEnabled = mock.PropertyMock(return_value=True)
            mock_startTime = mock.PropertyMock(return_value=_start)
            mock_N_ref = mock.PropertyMock(return_value=N_ref)
            mock_dt = mock.PropertyMock(return_value=_elapsed)
            mock_isSubstep = mock.PropertyMock(return_value=False)
            type(step0).loggingEnabled = mock_loggingEnabled
            type(step0).startTime = mock_startTime
            type(step0).N_ref = mock_N_ref
            type(step0).dt = mock_dt
            type(step0).isSubstep = mock_isSubstep

            mockGetStep.return_value = step0

            instance = _ProgressRecorder.model_construct(steps={key0: step0})
            instance._activeSteps = [step0]
            instance.stop(key0)

            assert not bool(instance._activeSteps)
            mockGetStep.assert_called_once_with(key0)
            step0.stop.assert_called_once()
            mock_exc_info.assert_called_once()
            mock_logCompletion.assert_called_once_with(key0, False)
            mock_datetime.now.assert_called_once()
            mock_startTime.assert_called_once()
            mock_N_ref.assert_called_once()
            mock_dt.assert_called_once()
            mock_isSubstep.assert_called_once()
            step0.details.N_ref.assert_not_called()
            step0.details.order.assert_not_called()
            step0.recordMeasurement.assert_called_once_with(dt_elapsed=_elapsed, dt_est=_elapsed, N_ref=N_ref)
            step0.estimate.dt.assert_not_called()

        # enabled: substep stack exception
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            Config_override("application.workflows_data.timing.max_measurements", 50),
            Config_override("application.workflows_data.timing.update_minimum_count", 10),
            Config_override("application.workflows_data.timing.update_threshold", 0.2),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "datetime") as mock_datetime,
            mock.patch.object(inspect.getmodule(_ProgressRecorder).sys, "exc_info") as mock_exc_info,
            mock.patch.object(_ProgressRecorder, "getStep") as mockGetStep,
            mock.patch.object(_ProgressRecorder, "_logCompletion") as mock_logCompletion,
        ):
            mock_datetime.now.return_value = _now
            mock_exc_info.return_value = (None, None, None)
            step0 = mock.Mock(
                spec=ProgressStep,
                details=mock.Mock(spec=_Step, key=key0, N_ref=mock.Mock(return_value=N_ref)),
                measurements=[],
                estimate=mock.Mock(spec=_Estimate),
                stop=mock.Mock(),
                recordMeasurement=mock.Mock(),
            )
            # mock the `_Step` properties
            mock_loggingEnabled = mock.PropertyMock(return_value=True)
            mock_startTime = mock.PropertyMock(return_value=_start)
            mock_N_ref = mock.PropertyMock(return_value=N_ref)
            mock_dt = mock.PropertyMock(return_value=_elapsed)
            type(step0).loggingEnabled = mock_loggingEnabled
            type(step0).startTime = mock_startTime
            type(step0).N_ref = mock_N_ref
            type(step0).dt = mock_dt
            mockGetStep.return_value = step0

            instance = _ProgressRecorder.model_construct(steps={key0: step0})
            instance._activeSteps = []
            with pytest.raises(RuntimeError, match="Usage error: `_activeSteps` stack underflow."):
                instance.stop(key0)

        # disabled: does nothing
        with (
            Config_override("application.workflows_data.timing.enabled", False),
            Config_override("application.workflows_data.timing.max_measurements", 50),
            Config_override("application.workflows_data.timing.update_minimum_count", 10),
            Config_override("application.workflows_data.timing.update_threshold", 0.2),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "datetime") as mock_datetime,
            mock.patch.object(inspect.getmodule(_ProgressRecorder).sys, "exc_info") as mock_exc_info,
            mock.patch.object(_ProgressRecorder, "getStep") as mockGetStep,
            mock.patch.object(_ProgressRecorder, "_logCompletion") as mock_logCompletion,
        ):
            mock_datetime.now.return_value = _now
            mock_exc_info.return_value = (None, None, None)
            step0 = mock.Mock(
                spec=ProgressStep,
                details=mock.Mock(
                    spec=_Step, key=key0, N_ref=mock.Mock(return_value=N_ref), order=mock.Mock(return_value=N)
                ),
                measurements=[],
                estimate=mock.Mock(spec=_Estimate),
                stop=mock.Mock(),
                recordMeasurement=mock.Mock(),
            )
            # mock the `_Step` properties
            mock_loggingEnabled = mock.PropertyMock(return_value=True)
            mock_startTime = mock.PropertyMock(return_value=_start)
            mock_N_ref = mock.PropertyMock(return_value=N_ref)
            mock_dt = mock.PropertyMock(return_value=_elapsed)
            type(step0).loggingEnabled = mock_loggingEnabled
            type(step0).startTime = mock_startTime
            type(step0).N_ref = mock_N_ref
            type(step0).dt = mock_dt
            mockGetStep.return_value = step0

            instance = _ProgressRecorder.model_construct(steps={key0: step0})
            instance.stop(key0)

            mockGetStep.assert_not_called()
            step0.stop.assert_not_called()
            mock_exc_info.assert_not_called()
            mock_logCompletion.assert_not_called()
            mock_datetime.now.assert_not_called()
            mock_startTime.assert_not_called()
            mock_N_ref.assert_not_called()
            mock_dt.assert_not_called()
            step0.recordMeasurement.assert_not_called()
            step0.details.N_ref.assert_not_called()
            step0.details.order.assert_not_called()
            step0.estimate.dt.assert_not_called()
            step0.estimate.update.assert_not_called()

        # does not record a measurement if `N_ref` could not be calculated
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            Config_override("application.workflows_data.timing.max_measurements", 50),
            Config_override("application.workflows_data.timing.update_minimum_count", 5),
            Config_override("application.workflows_data.timing.update_threshold", 0.2),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "datetime") as mock_datetime,
            mock.patch.object(inspect.getmodule(_ProgressRecorder).sys, "exc_info") as mock_exc_info,
            mock.patch.object(_ProgressRecorder, "getStep") as mockGetStep,
            mock.patch.object(_ProgressRecorder, "_logCompletion") as mock_logCompletion,
        ):
            mock_datetime.now.return_value = _now
            mock_exc_info.return_value = (None, None, None)
            step0 = mock.Mock(
                spec=ProgressStep,
                details=mock.Mock(
                    spec=_Step,
                    key=key0,
                    # When `N_ref` cannot be calculated `N_ref()` returns `None`:
                    #   however, what we care about for this test is the cached property value
                    #   which is set via the property mock initialization below.
                    N_ref=mock.Mock(return_value=mock.sentinel.N_ref),
                    order=mock.Mock(return_value=mock.sentinel.N),
                ),
                measurements=[],
                estimate=mock.Mock(spec=_Estimate),
                stop=mock.Mock(),
                recordMeasurement=mock.Mock(),
            )
            # mock the `_Step` properties
            mock_loggingEnabled = mock.PropertyMock(return_value=True)
            mock_startTime = mock.PropertyMock(return_value=_start)
            # return `None` => `N_ref` could not be calculated
            mock_N_ref = mock.PropertyMock(return_value=None)
            # `N_ref is None` => `dt is None`
            mock_dt = mock.PropertyMock(return_value=None)
            mock_isSubstep = mock.PropertyMock(return_value=False)
            type(step0).loggingEnabled = mock_loggingEnabled
            type(step0).startTime = mock_startTime
            type(step0).N_ref = mock_N_ref
            type(step0).dt = mock_dt
            type(step0).isSubstep = mock_isSubstep
            mockGetStep.return_value = step0

            instance = _ProgressRecorder.model_construct(steps={key0: step0})
            instance._activeSteps = [step0]
            instance.stop(key0)

            assert not bool(instance._activeSteps)
            mockGetStep.assert_called_once_with(key0)
            step0.stop.assert_called_once()
            mock_exc_info.assert_called_once()
            mock_logCompletion.assert_called_once_with(key0, False)
            mock_datetime.now.assert_called_once()
            mock_startTime.assert_called_once()
            mock_N_ref.assert_called_once()
            mock_dt.assert_called_once()
            step0.recordMeasurement.assert_not_called()
            step0.details.N_ref.assert_not_called()
            step0.details.order.assert_not_called()
            step0.estimate.dt.assert_not_called()
            step0.estimate.update.assert_not_called()

        # exception in progress: gets the step, calls `step.stop`
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            Config_override("application.workflows_data.timing.max_measurements", 50),
            Config_override("application.workflows_data.timing.update_minimum_count", 100),
            Config_override("application.workflows_data.timing.update_threshold", 0.2),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "datetime") as mock_datetime,
            mock.patch.object(inspect.getmodule(_ProgressRecorder).sys, "exc_info") as mock_exc_info,
            mock.patch.object(_ProgressRecorder, "getStep") as mockGetStep,
            mock.patch.object(_ProgressRecorder, "_logCompletion") as mock_logCompletion,
        ):
            mock_datetime.now.return_value = _now
            mock_exc_info.return_value = (RuntimeError, None, None)
            step0 = mock.Mock(
                spec=ProgressStep,
                details=mock.Mock(
                    spec=_Step,
                    key=key0,
                    N_ref=mock.Mock(return_value=mock.sentinel.N_ref),
                    order=mock.Mock(return_value=mock.sentinel.N),
                ),
                measurements=[],
                estimate=mock.Mock(spec=_Estimate),
                stop=mock.Mock(),
                recordMeasurement=mock.Mock(),
            )
            # mock the `_Step` properties
            mock_loggingEnabled = mock.PropertyMock(return_value=True)
            mock_startTime = mock.PropertyMock(return_value=_start)
            mock_N_ref = mock.PropertyMock(return_value=N_ref)
            mock_dt = mock.PropertyMock(return_value=_elapsed)
            type(step0).loggingEnabled = mock_loggingEnabled
            type(step0).startTime = mock_startTime
            type(step0).N_ref = mock_N_ref
            type(step0).dt = mock_dt
            mockGetStep.return_value = step0

            instance = _ProgressRecorder.model_construct(steps={key0: step0})
            instance._activeSteps = [step0]
            instance.stop(key0)

            assert not bool(instance._activeSteps)
            mockGetStep.assert_called_once_with(key0)
            # timing properties are always called prior to `step.stop()`
            mock_startTime.assert_called_once()
            mock_N_ref.assert_called_once()
            mock_dt.assert_called_once()
            step0.stop.assert_called_once()
            mock_exc_info.assert_called_once()
            mock_logCompletion.assert_not_called()
            mock_datetime.now.assert_not_called()
            step0.recordMeasurement.assert_not_called()
            step0.details.N_ref.assert_not_called()
            step0.details.order.assert_not_called()
            step0.estimate.dt.assert_not_called()
            step0.estimate.update.assert_not_called()


class TestWallClockTime:
    class _Class:
        attribute_one = 45.0

        def N_ref(self):
            pass

        def method_one(self):
            pass

    class _Class2:
        def method_two(self, *_args, **_kwargs):
            pass

    def test_decorator(self):
        with mock.patch.object(inspect.getmodule(_ProgressRecorder), "ProgressRecorder") as mockProgressRecorder:
            # `N_ref`, `order`, and `stepName` are optional
            decorator = WallClockTime()
            #   verify that default values are set
            assert decorator.N_ref is not None
            assert isinstance(decorator.N_ref, LambdaType)  # we can't compare a lambda directly
            assert decorator.order == ComputationalOrder.O_0
            assert decorator.stepName is None

            # `N_ref_args` must not be specified
            decorator = WallClockTime(
                N_ref=lambda *args, **kwargs: args[0] * args[1] * len(kwargs), N_ref_args=((1.0, 2.0), {"one": 1.0})
            )
            with pytest.raises(RuntimeError, match=".*`N_ref_args` must not be specified.*"):
                decorator(TestWallClockTime._Class.method_one)

            ## decorating a class ##
            decorator = WallClockTime(callerOverride="method_one")
            decorator(TestWallClockTime._Class)

            # -- specifying `N_ref` by name
            decorator = WallClockTime(callerOverride="method_one", N_ref="N_ref")
            decorator(TestWallClockTime._Class)

            # -- `N_ref` by name, must be a method of the class
            decorator = WallClockTime(callerOverride="method_one", N_ref="method_two")
            with pytest.raises(
                RuntimeError, match="Usage error.*\n.*N_ref.*\n.*the name of a method of the decorated class.*"
            ):
                decorator(TestWallClockTime._Class)

            #  -- `callerOverride` must be specified
            decorator = WallClockTime()
            with pytest.raises(RuntimeError, match=".*`callerOverride` must be specified.*"):
                decorator(TestWallClockTime._Class)

            #  -- `callerOverride` must be a string
            decorator = WallClockTime(callerOverride=TestWallClockTime._Class.method_one)
            with pytest.raises(
                RuntimeError, match=".*`callerOverride` must be the name of a method of the decorated class.*"
            ):
                decorator(TestWallClockTime._Class)

            #  -- `callerOverride` must be a method (i.e. not some other attribute)
            decorator = WallClockTime(callerOverride="attribute_one")
            with pytest.raises(
                RuntimeError, match=".*`callerOverride` must be the name of a method of the decorated class.*"
            ):
                decorator(TestWallClockTime._Class)

            #  -- `callerOverride` must actually be a method of the decorated class
            decorator = WallClockTime(callerOverride="method_two")
            with pytest.raises(
                RuntimeError, match=".*`callerOverride` must be the name of a method of the decorated class.*"
            ):
                decorator(TestWallClockTime._Class)

            ## decorating a function ##
            decorator = WallClockTime()
            decorator(TestWallClockTime._Class.method_one)

            # -- `N_ref` specified
            decorator = WallClockTime(N_ref=TestWallClockTime._Class.N_ref)
            decorator(TestWallClockTime._Class.method_one)

            # -- `N_ref` specified as function, not by name
            decorator = WallClockTime(N_ref="N_ref")
            with pytest.raises(RuntimeError, match="Usage error.*\n.*N_ref.*must be a function.*"):
                decorator(TestWallClockTime._Class.method_one)

            #  -- `callerOverride` must not be specified
            decorator = WallClockTime(callerOverride=TestWallClockTime._Class.method_one)
            with pytest.raises(RuntimeError, match=".*`callerOverride` must not be specified.*"):
                decorator(TestWallClockTime._Class.method_one)

            ## only `FunctionType` or `type` can be decorated ##
            decorator = WallClockTime()
            with pytest.raises(RuntimeError, match=".*only `FunctionType` or `type` can be decorated.*"):  # noqa: PT012
                # an attempt to decorate an instance
                instance = TestWallClockTime._Class()
                decorator(instance)

            ## decorated function can not be re-entrant ##
            decorator = WallClockTime()
            wrapped = None

            def _function(depth: int = 0):
                if depth < 2:
                    sleep(1)
                    wrapped(depth + 1)

            wrapped = decorator(_function)

            # Question: CAN THIS EVEN HAPPEN with a decorator?
            with pytest.raises(RuntimeError, match=".*decorated function is not re-entrant.*"):
                wrapped()

            ## verify that the wrapped function is profiled correctly
            with mock.patch.object(inspect.getmodule(_ProgressRecorder), "functools") as mock_functools:
                # disable `functools.wraps` decorator
                mock_functools.wraps = mock.Mock(return_value=lambda func: func)

                key = mock.sentinel.key
                mockProgressRecorder.reset_mock()
                mockProgressRecorder.record.return_value = key

                def decoratee_(*_args, **_kwargs):
                    sleep(1)

                decoratee = mock.Mock(spec=FunctionType, side_effect=decoratee_)

                decorator = WallClockTime(
                    N_ref=mock.sentinel.N_ref, order=mock.sentinel.order, stepName=mock.sentinel.stepName
                )
                decorated = decorator(decoratee)
                _args, _kwargs = ("one", "two"), {"three": "three", "four": "four"}
                decorated(*_args, **_kwargs)

                decoratee.assert_called_once_with(*_args, **_kwargs)
                mockProgressRecorder.record.assert_called_once_with(
                    stepName=mock.sentinel.stepName,
                    callerOrStackFrameOverride=decoratee,
                    N_ref=mock.sentinel.N_ref,
                    N_ref_args=(_args, _kwargs),
                    order=mock.sentinel.order,
                    enableLogging=False,
                )
                mockProgressRecorder.stop.assert_called_once_with(key)

            ## verify that a method of a class is profiled correctly
            key = mock.sentinel.key
            mockProgressRecorder.reset_mock()
            mockProgressRecorder.record.return_value = key

            with (
                mock.patch.object(inspect.getmodule(_ProgressRecorder), "functools") as mock_functools,
                mock.patch.object(
                    TestWallClockTime._Class2, "method_two", spec=FunctionType, __name__="method_two"
                ) as mock_method,
            ):
                # disable `functools.wraps` decorator
                mock_functools.wraps = mock.Mock(return_value=lambda func: func)

                decoratee = TestWallClockTime._Class2
                decorator = WallClockTime(
                    callerOverride="method_two",
                    N_ref=mock.sentinel.N_ref,
                    order=mock.sentinel.order,
                    stepName=mock.sentinel.stepName,
                )
                decorated = decorator(decoratee)
                instance = decorated()
                _args, _kwargs = ("one", "two"), {"three": "three", "four": "four"}
                instance.method_two(*_args, **_kwargs)

                mock_method.assert_called_once_with(instance, *_args, **_kwargs)
                mockProgressRecorder.record.assert_called_once_with(
                    stepName=mock.sentinel.stepName,
                    # step key is generated from the <class>, not from the <class method>
                    callerOrStackFrameOverride=decoratee,
                    N_ref=mock.sentinel.N_ref,
                    N_ref_args=((instance, *_args), _kwargs),
                    order=mock.sentinel.order,
                    enableLogging=False,
                )
                mockProgressRecorder.stop.assert_called_once_with(key)

    def test_context_manager(self):
        with (
            Config_override("application.workflows_data.timing.enabled", True),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "ProgressRecorder") as mockProgressRecorder,
        ):
            key = mock.sentinel.key
            mockProgressRecorder.record.return_value = key

            # `N_ref` and `order` are optional
            stepName = mock.sentinel.stepName
            manager = WallClockTime(stepName=stepName)
            assert manager.stepName == stepName

            def _function(*_args, **_kwargs):
                pass

            # `N_ref` may be a function
            stepName = mock.sentinel.stepName
            manager = WallClockTime(stepName=stepName, N_ref=_function)
            assert manager.stepName == stepName

            # `N_ref` should not be a string
            stepName = mock.sentinel.stepName
            manager = WallClockTime(stepName=stepName, N_ref="_function")
            assert manager.stepName == stepName

            # `stepName` must be specified
            mockProgressRecorder.reset_mock()

            def _function():
                with WallClockTime():
                    sleep(1)

            with pytest.raises(RuntimeError, match=".*`stepName` must be specified.*"):
                _function()

            # `callerOverride` must not be specified
            mockProgressRecorder.reset_mock()

            def _another_function():
                pass

            def _function():
                with WallClockTime(stepName=stepName, callerOverride=_another_function):
                    sleep(1)

            with pytest.raises(RuntimeError, match=".*`callerOverride` must not be specified.*"):
                _function()

            # context manager cannot be re-used
            mockProgressRecorder.reset_mock()

            def _function():
                manager = WallClockTime(stepName=stepName)
                with manager:
                    sleep(1)
                with manager:
                    sleep(1)

            with pytest.raises(RuntimeError, match=".*context manager cannot be re-used.*"):
                _function()

            # correctly profiles the wrapped section
            mockProgressRecorder.reset_mock()

            def _N_ref(*_args, **_kwargs):
                return mock.sentinel.N_ref

            _N_ref_args = (mock.sentinel.args, mock.sentinel.kwargs)
            _order = mock.sentinel.order

            def _function():
                with WallClockTime(stepName=stepName, N_ref=_N_ref, N_ref_args=_N_ref_args, order=_order):
                    sleep(1)

            _function()

            class IsFrameType:
                def __eq__(self, other):
                    return isinstance(other, FrameType)

            mockProgressRecorder.record.assert_called_once_with(
                stepName=stepName,
                # difficult to compare exactly: the calling stack frame was inside the `_function`
                callerOrStackFrameOverride=IsFrameType(),
                N_ref=_N_ref,
                N_ref_args=_N_ref_args,
                order=_order,
                enableLogging=False,
            )
            mockProgressRecorder.stop.assert_called_once_with(key)

        # disabled: does nothing
        with (
            Config_override("application.workflows_data.timing.enabled", False),
            mock.patch.object(inspect.getmodule(_ProgressRecorder), "ProgressRecorder") as mockProgressRecorder,
        ):

            def _N_ref(*_args, **_kwargs):
                return mock.sentinel.N_ref

            _N_ref_args = (mock.sentinel.args, mock.sentinel.kwargs)
            _order = mock.sentinel.order

            def _function():
                with WallClockTime(stepName=stepName, N_ref=_N_ref, N_ref_args=_N_ref_args, order=_order):
                    sleep(1)

            _function()

            mockProgressRecorder.record.assert_not_called()
            mockProgressRecorder.stop.assert_not_called()
