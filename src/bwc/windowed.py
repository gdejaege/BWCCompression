from abc import abstractmethod
from sortedcontainers import SortedList
import pandas as pd
from pymeos import TGeomPointSeq
from src.helpers.utility import PriorityPoint


class Windowed:
    def __init__(self, points, window_lenght, limit, nys):
        self.instants = points  # dataframe of points (can be with SOG, COG)
        self.window = window_lenght
        self.limit = limit
        self.nys = nys
        self.trips = {}  # trips # the points kept in the trips before the window
        # window related attributes
        self.window_trips = {}  # could be lists sorted by time !
        self.priority_list = SortedList(key=lambda x: x.priority)  # priorities!
        self.delays = []

    def compress(self):
        """Compress all the points (in different time windows)."""
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
        self.finalize_trips()

    @abstractmethod
    def add_point(self, point):
        pass

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

    def compute_delays(self, time):
        """Compute the delay between the reception and validation of the point."""
        for point in self.priority_list:
            self.delays.append((time - point.point.timestamp()).total_seconds())

    def finalize_trips(self):
        """Build TGeomPoint sequences from the kept points."""
        # check for errors -> shouldn't be
        for key, points in self.trips.items():
            i = 0
            flag = False
            for i in range(len(points) - 1):
                if points[i].point.timestamp() >= points[i + 1].point.timestamp():
                    flag = True
            if flag or len(points) == 0:
                print(key, points)
                print("Above")
        # traj is a list of PriorityPoints
        trips_dico = {
            key: TGeomPointSeq.from_instants([x.point for x in traj], upper_inc=True)
            for key, traj in self.trips.items()
        }

        self.trips = pd.DataFrame.from_dict(
            trips_dico, orient="index", columns=["trajectory"]
        )
