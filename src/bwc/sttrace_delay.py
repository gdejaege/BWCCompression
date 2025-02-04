from sortedcontainers import SortedList

# import pandas as pd
from src.bwc.delay import Delay
from src.helpers.utility import PriorityPoint, compute_SED

# from datetime import timedelta
# from pymeos import TGeomPointSeq


class BWC_STTrace_Delay(Delay):
    def __init__(self, points, window_length, limit, proj):
        super().__init__(points, window_length, limit, proj)
        self.last_points = {}


    def evaluate_point(self, point):
        before, after = self.find_neighboors(point)

        if before is None or after is None:
            return float("inf")
        return compute_SED(before.point, point.point, after.point, self.proj)
