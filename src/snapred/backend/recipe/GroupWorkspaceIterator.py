from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class GroupWorkspaceIterator:
    """
    Utility class that enables the pythonic iteration of a group workspace by workspace name.
    """

    def __init__(self, groupingWorkspaceName):
        self.mantidSnapper = MantidSnapper(None, self.__class__.__name__)
        self.groupingWorkspace = self.mantidSnapper.mtd[groupingWorkspaceName]
        self.index = 0
        return

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        if self.index < self.groupingWorkspace.getNumberOfEntries():
            ws = self.groupingWorkspace.getItem(self.index)
            self.index += 1
            return ws.name()
        else:
            raise StopIteration
