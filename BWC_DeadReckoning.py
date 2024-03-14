from sortedcontainers import SortedList
from shapely.geometry import Point
from pymeos.main.tpoint import TGeomPointSeq
import pandas as pd
import utility as u


class PriorityPoint:
    """
    Class wrapping a point to compute its priority.
    """

    def __init__(self, row):
        self.tid = row["id"]
        self.point = row["point"]  # TGeomInst
        self.priority = 0
        if hasattr(row, "sog"):
            self.sog = row["sog"]
            self.cog = row["cog"]


class BWC_DR:
    def __init__(self, points, window_lenght, limit, nys):
        self.instants = points  # dataframe of points (can be with SOG, COG)
        self.window = window_lenght
        self.limit = limit
        self.nys = nys
        # the points kept in the trips before the window
        self.trips: dict[int, list[PriorityPoint]] = {}
        # window related attributes
        self.priority_list = SortedList(key=lambda x: x.priority)  # priorities!
        self.window_trips = {}  # could be lists sorted by time !

    def compress(self):
        """Compress all the points (in different time windows)."""
        start = self.instants.iloc[0].point.timestamp()
        window_end = start + self.window

        for _, row in self.instants.iterrows():
            time = row.point.timestamp()
            if time > window_end:
                window_end = window_end + self.window
                self.next_window()
            self.add_point(PriorityPoint(row))

        # keep points of last window
        self.next_window()
        self.finalize_trips()

    def next_window(self):
        """Empty the priorityQueue to the kept points."""
        for trip in self.window_trips:
            self.trips.setdefault(trip, []).extend(self.window_trips[trip])

        self.priority_list = SortedList(key=lambda x: x.priority)  # priorities!
        self.window_trips = {}  # could be lists sorted by time !

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
        to_remove = self.priority_list.pop(0)
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
                nys=self.nys,
            )  
        elif index <= 1:
            previous = extended_trip[index - 1]
            return Point(self.nys(previous.point.value().x, previous.point.value().y))
        else:
            return u.get_expected_pos_anteprev(
                time=point.point.timestamp(),
                prev=extended_trip[index - 1],
                anteprev=extended_trip[index - 2],
                nys=self.nys,
            )

    def evaluate_point(self, point):
        """returns the distance between point and the expected position."""
        expected_pos = self.get_expected_pos(point)
        current = Point(self.nys(point.point.value().x, point.point.value().y))
        distance = expected_pos.distance(current)
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
        self.finalized_trips = pd.DataFrame.from_dict(trips_dico, orient="index", 
                                                      columns=["trajectory"])
