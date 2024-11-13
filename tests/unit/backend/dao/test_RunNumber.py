from pydantic import BaseModel
from snapred.backend.dao.state.RunNumber import RunNumber


class ParentOfRunNumber(BaseModel):
    runNumber: RunNumber


class TestRunNumber:
    num = "54321"

    def test_runNumber(self):
        runNumber = RunNumber(runNumber=self.num)
        assert runNumber == self.num

    def test_marshallingOfStrIntoRunNumber(self):
        num = self.num
        runNumber = RunNumber(runNumber=num)
        inst = ParentOfRunNumber(runNumber=num)

        assert runNumber == inst.runNumber
        assert runNumber.runNumber == 54321
        assert inst.runNumber.runNumber == 54321
        assert inst.runNumber == num

    def test_serialization(self):
        num = self.num
        inst = ParentOfRunNumber(runNumber=num)
        assert inst.dict()["runNumber"] == num

    def test_deserialization(self):
        num = self.num
        d = {"runNumber": num}
        inst = ParentOfRunNumber(runNumber=num)
        assert ParentOfRunNumber.parse_obj(d) == inst
