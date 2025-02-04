from sortedcontainers import SortedList

# import pandas as pd
from src.bwc.delay import Delay
from src.helpers.utility import PriorityPoint, compute_SED

# from datetime import timedelta
# from pymeos import TGeomPointSeq


class BWC_SQUISH_Delay(Delay):
    def __init__(self, points, window_length, limit, proj):
        super().__init__(points, window_length, limit, proj)
        self.last_points = {}


    def evaluate_point(self, point):
        """Same as sttrace."""
        before, after = self.find_neighboors(point)

        if before is None or after is None:
            return float("inf")
        return compute_SED(before.point, point.point, after.point, self.proj)

    def remove_point(self):
        """Remove point with least priority and update its neighboors' priorities."""
        # to_remove = self.priority_list.pop(0)
        to_remove = self.pop()
        tid = to_remove.tid

        trip = self.window_trips[tid]

        to_remove_index = trip.index(to_remove)
        del trip[to_remove_index]

        # update priority of the neighboors
        if to_remove_index > 0:
            previous = trip[to_remove_index - 1]
            self.priority_list.remove(previous)
            previous.priority += to_remove.priority
            self.priority_list.add(previous)

        if to_remove_index < len(trip):
            following = trip[to_remove_index]
            self.priority_list.remove(following)
            following.priority += to_remove.priority
            self.priority_list.add(following)

        # if this case is true, the two preceding couldn't as not > 0 
        # we were obliged to remove the only point of a trajectory (before last)
        # the last point becomes the first one
        if to_remove_index == 0 and len(self.trips.get(tid, []) + trip) == 0:
            if tid in self.last_points:
                new = self.last_points[tid]
                del self.last_points[tid]
                new.priority = float("inf")
                trip.append(new)
                self.priority_list.add(new)

