from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from PyQt5 import QtWidgets


@dataclass
class WorkflowNodeModel(object):
    view: QtWidgets.QWidget
    action: Callable
    nextModel: WorkflowNodeModel
