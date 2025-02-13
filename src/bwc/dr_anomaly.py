from sortedcontainers import SortedList
from shapely.geometry import Point
from pymeos.main.tpoint import TGeomPointSeq
import pandas as pd
from src.bwc.windowed import Windowed
import src.helpers.utility as u

from src.helpers.utility import PriorityPoint


class BWC_DR_anomaly_new(Windowed):
    def __init__(self, points, window_length, limit, proj, importance_ratio=2):
        self.importance_ratio = importance_ratio
        print(self.importance_ratio)
        self.anomalies = [set(), set(), set()]
        self.total_anomalies = 0
        super().__init__(points, window_length, limit, proj)

    def check_anomaly(self, row):
        if row["is_anomaly"]:
            self.anomalies[0].add(row["id"])
            self.total_anomalies += 1

    def compress(self):
        """Compress all the points (in different time windows)."""
        start = self.instants.iloc[0].point.timestamp()
        # gc.set_debug(gc.DEBUG_LEAK)
        # print("start", start)
        print(self.window, end=": ")
        window_end = start + self.window
        print_progress = self.print_progress()
        for _, row in self.instants.iterrows():
            self.check_anomaly(row)
            next(print_progress)
            time = row.point.timestamp()
            if time > window_end:
                self.next_window(window_end)
                self.anomalies = [set(), self.anomalies[0], self.anomalies[1]]
                window_end = window_end + self.window
            self.add_point(PriorityPoint(row))

        last_time = max([x.timestamp() for x in self.instants.point])
        self.next_window(last_time)
        self.finalize_trips()
        print("anomalies found", self.total_anomalies)


    def add_point(self, point):
        """Process the incoming point then remove from queue and update priorities."""
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

        elif hasattr(point, "sog"):
            return u.get_expected_pos_sog(
                start=extended_trip[index - 1],
                time=point.point.timestamp(),
                proj=self.proj,
            )
        elif index <= 1:
            previous = extended_trip[index - 1]
            return Point(self.proj(previous.point.value().x, previous.point.value().y))
        else:
            return u.get_expected_pos_anteprev(
                time=point.point.timestamp(),
                prev=extended_trip[index - 1],
                anteprev=extended_trip[index - 2],
                proj=self.proj,
            )

    def evaluate_point(self, point):
        """returns the distance between point and the expected position."""
        expected_pos = self.get_expected_pos(point) # returns a projected position!
        current = Point(self.proj(point.point.value().x, point.point.value().y))
        distance = expected_pos.distance(current)
        if point.tid in self.anomalies[0] or point.tid in self.anomalies[1] or point.tid in self.anomalies[2]:
            distance = distance*self.importance_ratio
        return distance

    def finalize_trips(self):
        """Build TGeomPoint sequences from the kept points."""
        # only to check if order  problem!:
        for key, points in self.trips.items():
            i = 0
            flag = False
            for i in range(len(points) - 1):
                if points[i].point.timestamp() >= points[i + 1].point.timestamp():
                    flag = True
            if flag or len(points) == 0:
                print("Above")
                print(key, len(points))

        # traj is a list of PriorityPoints
        trips_dico = {
            key: TGeomPointSeq.from_instants([x.point for x in traj], upper_inc=True)
            for key, traj in self.trips.items()
        }
        self.finalized_trips = pd.DataFrame.from_dict(
            trips_dico, orient="index", columns=["trajectory"]
        )
