from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from qtpy import QtWidgets

from snapred.backend.error.ContinueWarning import ContinueWarning


@dataclass
class WorkflowNodeModel(object):
    view: QtWidgets.QWidget
    continueAction: Callable
    nextModel: WorkflowNodeModel
    name: str = "Unnamed"
    required: bool = True
    iterate: bool = False
    continueAnywayHandler: Callable[[ContinueWarning.Model], None] = None

    def __iter__(self):
        return _WorkflowModelIterator(self)


class _WorkflowModelIterator:
    def __init__(self, model):
        self.model = model

    def __next__(self):
        if self.model is None:
            raise StopIteration
        else:
            model = self.model
            self.model = self.model.nextModel
            return model
