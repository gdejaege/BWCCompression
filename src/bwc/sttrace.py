from sortedcontainers import SortedList
# import pandas as pd
from src.bwc.windowed import Windowed
from src.helpers.utility import PriorityPoint, compute_SED

# from datetime import timedelta
# from pymeos import TGeomPointSeq


class BWC_STTrace(Windowed):
    def __init__(self, points, window_lenght, limit, nys):
        super().__init__(points, window_lenght, limit, nys)

    def add_point(self, point):
        """Process the incoming point then remove from queue and update priorities."""
        existing_len = len(self.window_trips.get(point.tid, [])) + len(
            self.trips.get(point.tid, [])
        )
        if existing_len > 0:
            point.priority = 1e20
        else:
            point.priority = float("inf")
        self.priority_list.add(point)
        self.window_trips.setdefault(point.tid, []).append(point)

        if len(self.window_trips[point.tid]) > 1:
            self.update_priority_antelast_point(point.tid)

        while len(self.priority_list) > self.limit:
            self.remove_point()

    def update_priority_antelast_point(self, tid):
        """Compute the priority (SED) of point before the new last one of trajectory."""
        trip = self.window_trips[tid]
        if tid not in self.trips and len(trip) == 2:
            # the antelast is the first, therefore we keep priority infinite
            return

        to_update = trip[-2]  # the window_trip size already been checked
        self.priority_list.remove(to_update)
        to_update.priority = self.evaluate_point(to_update)
        self.priority_list.add(to_update)
        return

    def remove_point(self):
        """Remove point with least priority and update its neighboors' priorities."""
        to_remove = self.priority_list.pop(0)
        tid = to_remove.tid

        trip = self.window_trips[tid]

        to_remove_index = trip.index(to_remove)
        del trip[to_remove_index]

        # update priority of the neighboors
        if to_remove_index > 0:
            previous = trip[to_remove_index - 1]
            self.priority_list.remove(previous)
            previous.priority = self.evaluate_point(previous)
            self.priority_list.add(previous)

        if to_remove_index < len(trip):
            following = trip[to_remove_index]
            self.priority_list.remove(following)
            following.priority = self.evaluate_point(following)
            self.priority_list.add(following)

    def evaluate_point(self, point):
        """Compute the priority (SED) of point before the new last one of trajectory."""
        tid = point.tid
        extended_trip = self.trips.get(tid, [])[-1:] + self.window_trips[tid]
        point_id = extended_trip.index(point)

        # it can happen if we deleted the first point in window (because already points before)
        # or the antelast one
        if point_id == 0 or point_id == len(extended_trip) - 1:
            return float("inf")
        else:
            return compute_SED(
                extended_trip[point_id - 1].point,
                point.point,
                extended_trip[point_id + 1].point,
                self.nys,
            )



def classical_STTrace(trips, instants, npoints, nys, delta):
    """Same but with 1 time window."""

    bwc_sttrace = BWC_STTrace(instants, window_lenght=delta, limit=npoints, nys=nys)
    bwc_sttrace.compress()
    return bwc_sttrace.trips
