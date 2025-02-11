from datetime import timedelta

from sortedcontainers import SortedList
from shapely.geometry import Point
from pymeos.main.tpoint import TGeomPointSeq
import pandas as pd
from src.bwc.windowed import Windowed
import src.helpers.utility as u
import random

from src.helpers.utility import PriorityPoint


class BWC_DR_Anomaly():
    """For mobispaces only. Does not extend windows because of the anomalies. Work only with AIS"""
    def __init__(self, points, window_length, limit, proj, anomaly_threshold):
        self.instants = points  # dataframe of points (can be with SOG, COG)
        self.window = window_length
        self.limit = limit
        self.proj = proj
        self.trips = {}  # trips # the points kept in the trips before the window
        # window related attributes
        self.window_trips = {}  # could be lists sorted by time !
        self.priority_list = SortedList(key=lambda x: x.priority)  # priorities!
        self.delays = []
        self.finalized_trips: pd.DataFrame
        random.seed(0)
        self.anomalies = {}
        self.anomaly_threshold = anomaly_threshold

    def add_point(self, point):
        """Process the incoming point then remove from queue and update priorities."""
        # priority = self.evaluate_point(point)
        point.priority = float("inf")
        self.priority_list.add(point)
        self.window_trips.setdefault(point.tid, []).append(point)
        len_extended = len(self.trips.get(point.tid, [])) + len(
            self.window_trips[point.tid]
        )

        if (len_extended > 1 and hasattr(point, "sog")) or (len_extended > 2):
            self.update_priority_last_point(point)

        while len(self.priority_list) > self.limit:
            self.remove_point()

        while (len(self.priority_list) >= 1 and self.priority_list[0].priority < 1e-3):
            print("to low_priority", self.priority_list[0].priority, len(self.priority_list))
            self.remove_point()


    def update_priority_last_point(self, point):
        """Update the priority of the "previous" last point after adding a new point."""
        self.priority_list.remove(point)
        point.priority = self.evaluate_point(point)
        self.priority_list.add(point)
        return

    def remove_point(self):
        """Remove point with least priority and update its neighboors' priorities."""
        # to_remove = self.priority_list.pop(0)
        to_remove = self.pop()
        tid = to_remove.tid
        trip = self.window_trips[tid]
        to_remove_index = trip.index(to_remove)
        del trip[to_remove_index]

        to_update_index = to_remove_index
        while to_update_index < min(len(trip), to_remove_index + 2):
            to_update = trip[to_remove_index]
            self.priority_list.remove(to_update)
            if to_update_index + len(self.trips.get(tid, [])) == 0:
                to_update.priority = float("inf")
            else:
                to_update.priority = self.evaluate_point(to_update)
            self.priority_list.add(to_update)
            to_update_index += 1

    def get_expected_pos(self, point) -> Point:
        """Find the expected position:
        The expected position found extrapolating current trip until point.timestamp.
        """
        tid = point.tid
        extended_trip = self.trips.get(tid, [])[-2:] + self.window_trips[tid]
        index = extended_trip.index(point)

        if index == 0:
            # This is bad news, we had to update a point was the first of the trajectory
            # Shouldn't happen and not witnessed yet.
            print("bad new:", point.priority)
            return point  # float("inf") modified for type setting

        else:
            if extended_trip[index - 1].point.timestamp() - point.point.timestamp() < timedelta(seconds=1):
                return point.point.value()
            return u.get_expected_pos_sog(
                start=extended_trip[index - 1],
                time=point.point.timestamp(),
                proj=self.proj,
            )


    def evaluate_point(self, point):
        """returns the distance between point and the expected position."""
        factor = 1
        if self.same_point_already_in(point.tid, point.point.timestamp()):
            factor = 0
        elif self.close_to_anomaly(point.tid, point.point.timestamp()):
            factor = 100
        expected_pos = self.get_expected_pos(point)
        current = Point(self.proj(point.point.value().x, point.point.value().y))
        distance = expected_pos.distance(current)
        if distance <= 1e-3:
            print("distance:", distance)
        return distance*factor

    def same_point_already_in(self, tid, timestamp):
        kept_points = self.trips.get(tid, [])
        if timestamp in [pt.point.timestamp() for pt in kept_points]:
            return True
        for pt in self.priority_list:
            if pt.tid == tid and pt.point.timestamp() == timestamp and pt.priority == float("inf"):
                return True
        return False



    def finalize_trips(self):
        """Build TGeomPoint sequences from the kept points."""
        # only to check if order  problem!:
        to_remove = []
        for key, points in self.trips.items():
            i = 0
            flag = False
            for i in range(len(points) - 1):
                if points[i].point.timestamp() >= points[i + 1].point.timestamp():
                    flag = True
            if flag or len(points) == 0:
                print("Above")
                print(key, len(points), points)
                to_remove.append(key)
        # To correct!
        for key in to_remove:
            self.trips.pop(key)

        # traj is a list of PriorityPoints
        trips_dico = {
            key: TGeomPointSeq.from_instants([x.point for x in traj], upper_inc=True)
            for key, traj in self.trips.items()
        }
        self.finalized_trips = pd.DataFrame.from_dict(
            trips_dico, orient="index", columns=["trajectory"]
        )

    def compress(self):
        """Compress all the points (in different time windows)."""
        start = self.instants.iloc[0].point.timestamp()
        # gc.set_debug(gc.DEBUG_LEAK)
        print("start", start)
        print(self.window)
        window_end = start + self.window
        print_progress = self.print_progress()
        for _, row in self.instants.iterrows():
            self.check_anomaly(row)
            next(print_progress)
            time = row.point.timestamp()
            if time > window_end:
                self.next_window(window_end)
                window_end = window_end + self.window
            self.add_point(PriorityPoint(row))


        # keep points of last window
        print()
        last_time = max([x.timestamp() for x in self.instants.point])
        self.next_window(last_time)
        self.finalize_trips()

    def print_progress(self):
        n = len(self.instants)
        t = 0.1
        it = 0
        while True:
            if it / n > t:
                print(int(t * 10 + 0.01), end=" ", flush=True)
                t += 0.1
            it += 1
            yield

    def next_window(self, time):
        """Empty the priorityQueue to the kept points."""
        self.compute_delays(time)
        added = 0
        for trip in self.window_trips:
            self.trips.setdefault(trip, []).extend(self.window_trips[trip])
            added += len(self.window_trips[trip])

        self.priority_list = SortedList(key=lambda x: x.priority)  # priorities!
        self.window_trips = {}  # could be lists sorted by time !
        # the priorities buffered at the end are valid for next window start

    def pop(self):
        index = 0
        fp = self.priority_list[0]
        if fp.priority == float("inf"):
            index = random.randint(0, len(self.priority_list) - 1)
        return self.priority_list.pop(index)

    def compute_delays(self, time):
        """Compute the delay between the reception and validation of the point."""
        for point in self.priority_list:
            self.delays.append((time - point.point.timestamp()).total_seconds())

    def check_anomaly(self, row):
        if row["is_anomaly"]:
            self.anomalies.setdefault(row["id"], []).append(row["point"].timestamp())

    def close_to_anomaly(self, tid, timestamp):
        res = False
        for anomaly_time in self.anomalies.get(tid, []):
            if timestamp > anomaly_time and timestamp - anomaly_time <= self.anomaly_threshold:
                res = True
                break
        return res