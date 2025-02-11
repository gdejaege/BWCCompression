import random

from shapely.geometry import Point
from pymeos.main.tpoint import TGeomPointSeq
import pandas as pd
from src.bwc.windowed import Windowed
import src.helpers.utility as u



class BWC_Random():
    def __init__(self, points, window_length, limit, proj):
        # super().__init__(points, window_length, limit, proj)
        self.instants = points
        self.window = window_length
        self.limit = limit
        self.proj = proj
        self.delays = []


    def compress(self):
        """Compress all the points (in different time windows)."""
        start = self.instants.iloc[0].point.timestamp()
        end = max([pt.timestamp() for pt in self.instants["point"]])
        window_end = start + self.window

        self.kept_points = []
        print("until", (end - start).total_seconds()/3600, end=":")
        prev = None
        window_points = []
        for _, row in self.instants.iterrows():
            time = row.point.timestamp()
            if time > window_end:
                if (time - start).seconds // 3600 != prev:
                    print((time - start).seconds // 3600, end="")
                    prev = (time - start).seconds // 3600
                selected = random.sample(window_points, min(self.limit, len(window_points)))
                self.kept_points.extend(selected)
                self.compute_delays(selected, time)
                window_points = []
                window_end = window_end + self.window
            window_points.append(row)
        print()
        last_time = max([x.timestamp() for x in self.instants.point])
        selected = random.sample(window_points, min(self.limit, len(window_points)))
        self.kept_points.extend(selected)
        self.finalize_trips()

    def finalize_trips(self):
        """Build TGeomPoint sequences from the kept points."""
        # only to check if order  problem!:
        trajectories = {}
        for row in self.kept_points:
            trip_id = row["id"]
            point = row["point"]
            trajectories.setdefault(trip_id, []).append(point)

        for traj, points in trajectories.items():
            trajectories[traj] = sorted(points, key=lambda p: p.timestamp())

        # traj is a list of PriorityPoints
        trips_dico = {
            key: TGeomPointSeq.from_instants(traj, upper_inc=True)
            for key, traj in trajectories.items()
        }

        self.finalized_trips = pd.DataFrame.from_dict(
            trips_dico, orient="index", columns=["trajectory"]
        )

    def compute_delays(self, selected, time):
        for row in selected:
            self.delays.append((time - row.point.timestamp()).total_seconds())
