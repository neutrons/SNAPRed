from typing import Tuple

from mantid.kernel import V3D, Quat


def tupleFromV3D(v: V3D) -> Tuple[float, float, float]:
    return tuple([v[n] for n in range(3)])


def tupleFromQuat(q: Quat) -> Tuple[float, float, float, float]:
    return tuple([q[n] for n in range(4)])
