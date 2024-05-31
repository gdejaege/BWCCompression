from sortedcontainers import SortedList

import pandas as pd
from src.helpers.utility import PriorityPoint, compute_SED, convert_trips_points
from src.bwc.windowed import Windowed

from pymeos import TGeomPointSeq


class BWC_SQUISH(Windowed):
    def __init__(self, points, window_lenght, limit, nys):
        super().__init__(points, window_lenght, limit, nys)
        self.end_priorities = {} # buffered priorities to add to last point

    def next_window(self, time):
        """Empty the priorityQueue to the kept points."""
        super().next_window(time)
        self.end_priorities = {}

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

        # if there is a buffered priorities at the end, we add it
        # For instance if we have the trajectory "a b c d" to which e will be added,
        # if for some reason c was dropped before the addition of e, we add the
        # priority of c to d.
        if tid in self.end_priorities:
            to_update.priority += self.end_priorities.pop(tid)

        self.priority_list.add(to_update)
        return

    def remove_point(self):
        """Remove point with least priority and update its neighboors' priorities."""
        to_remove = self.priority_list.pop(0)
        tid = to_remove.tid
        trip = self.window_trips[tid]
        to_remove_index = trip.index(to_remove)
        del trip[to_remove_index]

        # update priority of the neighboors using SQUISH heuristic
        if to_remove_index > 0:
            previous = trip[to_remove_index - 1]
            self.priority_list.remove(previous)
            previous.priority += to_remove.priority
            self.priority_list.add(previous)

        if to_remove_index < len(trip):
            following = trip[to_remove_index]  # since node already removed
            self.priority_list.remove(following)
            following.priority += to_remove.priority
            self.priority_list.add(following)
        else:
            # if there is no following we buffer the priority:
            self.end_priorities[tid] = (
                self.end_priorities.get(tid, 0) + to_remove.priority
            )

    def evaluate_point(self, point):
        """returns the original SED evaluation."""
        tid = point.tid
        full_trip = self.trips.get(tid, [])[-1:] + self.window_trips[tid]
        point_id = full_trip.index(point)

        # normally it should not happen because in squish the evaluation is done only at first insertion
        # after, the updating is done by adding the deleted point priority
        if point_id == 0 or point_id == len(full_trip):
            # print("error")
            return float("inf")
        else:
            return compute_SED(
                full_trip[point_id - 1].point,
                point.point,
                full_trip[point_id + 1].point,
                self.nys,
            )



def classical_squish(trips, ratio, delta, nys):
    """Same but with only 1 time window."""
    res = {}
    for mmsi, row in trips.iterrows():
        trajectory = row.trajectory
        nb_points = max(len(trajectory.instants()) // ratio, 3)
        points = convert_trips_points(mmsi, trajectory)

        bwc_squish = BWC_SQUISH(points, window_lenght=delta, limit=nb_points, nys=nys)
        bwc_squish.compress()
        res_mmsi = bwc_squish.trips

        for mmsi, row in res_mmsi.iterrows():
            res[mmsi] = row.trajectory

    results = pd.DataFrame.from_records(
        ((mmsi, trajectory) for mmsi, trajectory in res.items()),
        columns=["id", "trajectory"],
        index="id",
    )
    return results
