from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from PyQt5 import QtWidgets


@dataclass
class WorkflowNodeModel(object):
    view: QtWidgets.QWidget
    continueAction: Callable
    nextModel: WorkflowNodeModel
    name: str = "Unnamed"

    # define iterator to iterate over all models
    def __iter__(self):
        yield self
        if self.nextModel is not None:
            yield from self.nextModel
