from PyQt5 import QtGui, QtCore, QtWidgets
import sys

from snapred.ui.widget.MainWidget import DummyWidget

import snapred as sr
import inspect

# print(inspect.getmembers(sr, predicate=inspect.ismodule))

is_namespace = (
    lambda module: hasattr(module, "__path__")
    and getattr(module, "__file__", None) is None
)

def get_all_module_files(modules=None):
    module_files = []
    while len(modules) > 0:
        module = modules.pop()
        if is_namespace(module[1]):
            modules.extend(inspect.getmembers(module[1], predicate=inspect.ismodule))
        else:
            module_files.append(module[1])
        
        # import pdb ; pdb.set_trace()

    return module_files

modules = get_all_module_files([("snapred", sr)])

print(modules)

for module in modules:
    print(inspect.getmembers(module, predicate=inspect.isclass))


class DummyGUI(QtWidgets.QMainWindow):

    def __init__(self, parent=None):
        super(DummyGUI, self).__init__(parent)

        dummyWidget = DummyWidget("load dummy", self)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        splitter.addWidget(dummyWidget.widget)

        self.setCentralWidget(splitter)
        self.setWindowTitle("DummyGUI")


def qapp():
    if QtWidgets.QApplication.instance():
        _app = QtWidgets.QApplication.instance()
    else:
        _app = QtWidgets.QApplication(sys.argv)
    return _app


if __name__ == "__main__":
    app = qapp()
    try:
        ex = DummyGUI()
        #ex.resize(700, 700)
        ex.show()
        app.exec_()
    except RuntimeError as error:
        ex = QtWidgets.QWidget()
        QtWidgets.QMessageBox.warning(ex, "Error", str(error))