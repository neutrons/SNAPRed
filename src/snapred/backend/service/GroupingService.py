from typing import Dict, List

from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class GroupingService:
    """Service for grouping-related validation such as detector-panel straddle checks."""

    _straddleCache: Dict[str, bool] = {}

    def __init__(self, groceryService: GroceryService = None) -> None:
        self.groceryService = groceryService or GroceryService()
        self.mantidSnapper = MantidSnapper(None, __name__)

    def isGroupNonStraddling(self, groupName: str, runId: str, useLiteMode: bool) -> bool:
        """
        Return True if the named grouping does NOT straddle both the East and
        West detector panels.  Results are cached by group name.
        """
        if groupName not in self._straddleCache:
            item = GroceryListItem.builder().grouping(groupName).fromRun(runId).useLiteMode(useLiteMode).build()
            wsName = self.groceryService.fetchGroupingDefinition(item)["workspace"]
            straddlingPtr = self.mantidSnapper.DetectorPanelStraddleCheck(
                "Checking panel straddle", GroupingWorkspace=wsName
            )
            self.mantidSnapper.executeQueue()
            straddling = straddlingPtr.get()
            self._straddleCache[groupName] = len(straddling) == 0

        return self._straddleCache[groupName]

    def filterCalibrationRecordsByNonStraddlingGroups(
        self,
        records: List[CalibrationRecord],
        runId: str,
        useLiteMode: bool,
    ) -> List[CalibrationRecord]:
        """
        Return only the CalibrationRecords whose focus group does not straddle
        both the East and West detector panels.
        """
        kept: List[CalibrationRecord] = []
        for record in records:
            groupName = record.focusGroupCalibrationMetrics.focusGroupName
            if self.isGroupNonStraddling(groupName, runId, useLiteMode):
                kept.append(record)
            else:
                logger.info(
                    f"Excluding calibration record (run={record.runNumber}, version={record.version}) "
                    f"because grouping '{groupName}' straddles East/West panels"
                )
        return kept

    def filterNormalizationRecordsByNonStraddlingGroups(
        self,
        records: List[NormalizationRecord],
        runId: str,
        useLiteMode: bool,
    ) -> List[NormalizationRecord]:
        """
        Return only the NormalizationRecords whose focus group does not straddle
        both the East and West detector panels.
        """
        kept: List[NormalizationRecord] = []
        for record in records:
            groupName = record.calculationParameters.name
            if self.isGroupNonStraddling(groupName, runId, useLiteMode):
                kept.append(record)
            else:
                logger.info(
                    f"Excluding normalization record (run={record.runNumber}, version={record.version}) "
                    f"because grouping '{groupName}' straddles East/West panels"
                )
        return kept
