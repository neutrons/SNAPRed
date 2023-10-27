# Use this script to test LiteDataCreationAlgo.py
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np

from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo

LDCA = LiteDataCreationAlgo()
LDCA.initialize()
LDCA.setProperty("InputWorkspace", "47278_raw")
LDCA.setProperty("RunNumber", "47278")
LDCA.setProperty("OutputWorkspace", "47278_lite")
LDCA.execute()