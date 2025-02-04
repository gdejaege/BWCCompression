from abc import abstractmethod
from sortedcontainers import SortedList
import pandas as pd
from pymeos import TGeomPointSeq
import pymeos
from src.helpers.utility import PriorityPoint
import gc
import random


class Windowed:
    def __init__(self, points, window_lenght, limit, proj):
        self.instants = points  # dataframe of points (can be with SOG, COG)
        self.window = window_lenght
        self.limit = limit
        self.proj = proj
        self.trips = {}  # trips # the points kept in the trips before the window
        # window related attributes
        self.window_trips = {}  # could be lists sorted by time !
        self.priority_list = SortedList(key=lambda x: x.priority)  # priorities!
        self.delays = []
        self.finalized_trips: pd.DataFrame
        random.seed(0)

    def compress(self):
        """Compress all the points (in different time windows)."""
        start = self.instants.iloc[0].point.timestamp()
        # gc.set_debug(gc.DEBUG_LEAK)
        print("start", start)
        print(self.window)
        window_end = start + self.window
        print_progress = self.print_progress()
        for _, row in self.instants.iterrows():
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
                # print(len(gc.garbage))
                t += 0.1
                # # try to reduce memory impact:
                # print("test finalizing")
                # pymeos.pymeos_finalize()
                # print("pymeos closed")
                # pymeos.pymeos_initialize()
                # print("test passed")
            it += 1
            yield

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

        self.finalized_trips = pd.DataFrame.from_dict(
            trips_dico, orient="index", columns=["trajectory"]
        )
