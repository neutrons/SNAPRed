from snapred.meta.Enum import StrEnum
from util.script_as_test import not_a_test

@not_a_test
class TestSummary:
    def __init__(self):
        self._index = 0
        self._steps = []

    def SUCCESS(self):
        step = self._steps[self._index]
        step.status = self.TestStep.StepStatus.SUCCESS
        self._index += 1

    def FAILURE(self):
        step = self._steps[self._index]
        step.status = self.TestStep.StepStatus.FAILURE
        self._index += 1

    def isComplete(self):
        return self._index == len(self._steps)

    def isFailure(self):
        return any(step.status == self.TestStep.StepStatus.FAILURE for step in self._steps)

    def builder():
        return TestSummary.TestSummaryBuilder()

    def __str__(self):
        longestStatus = max(len(step.status) for step in self._steps)
        longestName = max(len(step.name) for step in self._steps)
        tableCapStr = "#" * (longestName + longestStatus + 6)
        tableStr = (
            f"\n{tableCapStr}\n"
            + "\n".join(f"# {step.name:{longestName}}: {step.status:{longestStatus}} #" for step in self._steps)
            + f"\n{tableCapStr}\n"
        )
        return tableStr

    class TestStep:
        class StepStatus(StrEnum):
            SUCCESS = "SUCCESS"
            FAILURE = "FAILURE"
            INCOMPLETE = "INCOMPLETE"

        def __init__(self, name: str):
            self.name = name
            self.status = self.StepStatus.INCOMPLETE

    class TestSummaryBuilder:
        def __init__(self):
            self.summary = TestSummary()

        def step(self, name: str):
            self.summary._steps.append(TestSummary.TestStep(name))
            return self

        def build(self):
            return self.summary
