from sortedcontainers import SortedList
from src.bwc.delay import Delay
from src.helpers.utility import PriorityPoint, compute_SED
from pymeos import TGeomPointSeq
from pymeos.main.tpoint import TGeomPointInst, TGeomPointSeq
import haversine
import pandas as pd


class BWC_STTrace_Imp_Delay(Delay):
    def __init__(self, points, window_length, limit, proj, eval_delta, trips):
        super().__init__(points, window_length, limit, proj)
        self.eval_delta = eval_delta
        self.init_trips = trips
        self.last_points = {}


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

