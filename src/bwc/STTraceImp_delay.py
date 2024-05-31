from sortedcontainers import SortedList
from src.bwc.windowed import Windowed
from src.helpers.utility import PriorityPoint, compute_SED
from pymeos import TGeomPointSeq
from pymeos.main.tpoint import TGeomPointInst, TGeomPointSeq
import haversine
import pandas as pd


class BWC_STTrace_Imp_Delay(Windowed):
    def __init__(self, points, window_lenght, limit, nys, eval_delta, init_trips):
        super().__init__(points, window_lenght, limit, nys)
        self.eval_delta = eval_delta
        self.init_trips = init_trips
        self.last_points = {}

    def add_point(self, point):
        """Process the incoming point then remove from queue and update priorities."""
        tid = point.tid
        existing = self.window_trips.get(point.tid, []) + self.trips.get(point.tid, [])

        if len(existing) == 0:
            point.priority = float("inf")
            self.priority_list.add(point)
            self.window_trips.setdefault(point.tid, []).append(point)

        elif tid not in self.last_points:
            self.last_points[tid] = point

        else:
            old_last = self.last_points[tid]
            self.last_points[tid] = point
            self.window_trips.setdefault(tid, []).append(old_last)
            old_last.priority = self.evaluate_point(old_last)
            self.priority_list.add(old_last)

        while len(self.priority_list) > self.limit:
            self.remove_point()

    def find_neighboors(self, point):
        """We cannot suppose that there exist always a previous"""
        tid = point.tid

        window_trip = self.window_trips.get(tid, [])
        point_window_id = window_trip.index(point)

        if point_window_id == 0 and tid not in self.trips:
            previous = None
        elif point_window_id == 0:
            previous = self.trips[tid][-1]
        else:
            previous = window_trip[point_window_id - 1]

        if point_window_id == len(window_trip) - 1:
            next = self.last_points[tid]
        else:
            next = window_trip[point_window_id + 1]
        return next, previous

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
            before_previous, after_previous = self.find_neighboors(previous)
            if before_previous is not None:
                self.priority_list.remove(previous)
                previous.priority = self.evaluate_point(previous)
                self.priority_list.add(previous)

        if to_remove_index < len(trip):
            following = trip[to_remove_index]
            self.priority_list.remove(following)
            before_following, after_following = self.find_neighboors(following)
            following.priority = self.evaluate_point(following)
            self.priority_list.add(following)

        # if this case is true, the two preceding couldn't
        if to_remove_index == 0 and len(self.trips.get(tid, []) + trip) == 0:
            # we were obliged to remove the only point of a trajectory (before last)
            if tid in self.last_points:
                new = self.last_points[tid]
                del self.last_points[tid]
                new.priority = float("inf")
                trip.append(new)
                self.priority_list.add(new)

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
        last = [self.last_points[tid]] if tid in self.last_points else []
        extended_trip = (
            self.trips.get(tid, [])[-1:]
            + self.window_trips[tid]
            + last
        )
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

    def compress(self):
        """Compress all the points (in different time windows).

        We need to redefine because of the last time in finalize trips.
        """
        start = self.instants.iloc[0].point.timestamp()
        window_end = start + self.window
        for _, row in self.instants.iterrows():
            time = row.point.timestamp()
            if time > window_end:
                window_end = window_end + self.window
                self.next_window(time)
            self.add_point(PriorityPoint(row))

        # keep points of last window
        last_time = max([x.timestamp() for x in self.instants.point])
        self.next_window(last_time)
        self.finalize_trips_last_time(last_time)

    def finalize_trips_last_time(self, last_time):
        for tid, point in self.last_points.items():
            self.trips.setdefault(tid, []).append(point)
            self.delays.append(
                (last_time  -point.point.timestamp()).total_seconds()
            )

        return super().finalize_trips()
