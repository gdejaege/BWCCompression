import random

from shapely.geometry import Point
from pymeos.main.tpoint import TGeomPointSeq
import pandas as pd
from src.bwc.windowed import Windowed
import src.helpers.utility as u



class BWC_uniform():
    def __init__(self, points, window_length, limit, proj):
        # super().__init__(points, window_length, limit, proj)
        self.instants = points
        self.delta = window_length/limit
        self.proj = proj
        self.delays = []


    def compress(self):
        """Compress all the points (in different time windows)."""
        start = self.instants.iloc[0].point.timestamp()
        next_time = start

        self.kept_points = []
        window_points = []
        for _, row in self.instants.iterrows():
            time = row.point.timestamp()
            if time >= next_time:
                print((time - start).days, end="")
                self.kept_points.append(row)
                next_time += self.delta
        print()
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
