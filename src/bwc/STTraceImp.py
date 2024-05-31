from sortedcontainers import SortedList
from src.bwc.windowed import Windowed
from src.helpers.utility import PriorityPoint, compute_SED
from pymeos import TGeomPointSeq
from pymeos.main.tpoint import TGeomPointInst, TGeomPointSeq
import haversine
import pandas as pd


class BWC_STTrace_Imp(Windowed):
    def __init__(self, points, window_lenght, limit, nys, eval_delta, init_trips):
        super().__init__(points, window_lenght, limit, nys)
        self.eval_delta = eval_delta
        self.init_trips = init_trips


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
        """returns the original SED evaluation."""

        def distance_instant_line(instant, line):
            synchronized_point = line.value_at_timestamp(instant.timestamp())
            point = instant.value()
            return (
                haversine.haversine(
                    (point.y, point.x), (synchronized_point.y, synchronized_point.x)
                )
                * 1000
            )

        def distance_point_line_time(point, time, line):
            """Distance between Point and the value of the line at specific time."""
            synchronized_point = line.value_at_timestamp(time)
            return (
                haversine.haversine(
                    (point.y, point.x), (synchronized_point.y, synchronized_point.x)
                )
                * 1000
            )

        tid = point.tid
        extended_trip = self.trips.get(tid, [])[-1:] + self.window_trips[tid]
        point_id = extended_trip.index(point)

        # normally it should not happen
        if point_id == 0 or point_id == len(extended_trip) - 1:
            return float("inf")

        previous, following = (
            extended_trip[point_id - 1].point,
            extended_trip[point_id + 1].point,
        )

        old_curve = TGeomPointSeq.from_instants([previous, point.point, following])
        new_curve = TGeomPointSeq.from_instants([previous, following])

        old_error, new_error = 0, 0

        time = previous.timestamp() + self.eval_delta
        end = following.timestamp()

        if time >= end:
            return 0

        correct_trip = self.init_trips.loc[tid].trajectory

        while time < end:
            correct_point = correct_trip.value_at_timestamp(time)
            new_error += distance_point_line_time(correct_point, time, new_curve)
            old_error += distance_point_line_time(correct_point, time, old_curve)
            time += self.eval_delta
        return new_error - old_error

    def finalize_trips(self):
        """Build TGeomPoint sequences from the kept points."""
        # traj is a list of PriorityPoints
        trips_dico = {
            key: TGeomPointSeq.from_instants([x.point for x in traj], upper_inc=True)
            for key, traj in self.trips.items()
        }

        #
        self.trips = pd.DataFrame.from_dict(
            trips_dico, orient="index", columns=["trajectory"]
        )
