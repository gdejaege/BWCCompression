from typing import Dict, List

from pymeos import *
from pymeos.main.tpoint import TGeomPointInst, TGeomPointSeq

from PriorityQueue import *

from datetime import datetime, timedelta

import pandas as pd

from utility import *

class BandwidthConstraintCompressor():
    """
    Class performing the full pipeline of the trajectory compression.
    """
    
    def __init__(self, window_size=timedelta(minutes=15), nb_points=25, result_table=None, 
                 originTable=None, instants=None, 
                 trips=None,  # should be a dataframe with a column trajectory
                 database="mobilityDB"):
        self.result_table = result_table
        self.window_size = window_size
        self.nb_points = nb_points
        self.init_instants(originTable, instants)
        self.compute_start_end()
        self.initial_trips = trips if trips is not None else self.compute_trips()
        
 

    def init_instants(self, originTable, instants):
        """Returns a dataframe of mmsi, TGeomInst sorted by time"""
        if instants is not None:
            self.instants = instants
        
    def compute_start_end(self):
        self.start, self.end = self.instants.iloc[0].point.timestamp(), self.instants.iloc[-1].point.timestamp()
        
    
    def aggregate(self, increment:Dict[int, List[TGeomPointInst]]):
        for trajectory in increment:
            if trajectory in self.kept_points:
                self.kept_points[trajectory] += increment[trajectory]
            else:
                self.kept_points[trajectory] = increment[trajectory]


    def compress(self):
        self.trips = {}         # Dict[int, TGeomPointSeq 
        self.kept_points = {}   # Dict[int, List[TGeomPointInst]] 

        self.queue = PriorityQueue(mx=self.nb_points)
        window_start = self.start
        window_end = self.start + self.window_size
        
        for key, row in self.instants.iterrows():
            if row.point.timestamp() < window_start:   # should not happen here: start defined from points
                continue  
            while window_end < row.point.timestamp() < self.end:   # window shifting
                results_window = self.queue.get_trajectories()
                qty = sum([len(x) for x in results_window.values()])
                print("window:", window_start, window_end, qty)

                self.aggregate(results_window)
                window_start += self.window_size

                window_end = min(window_end+self.window_size, self.end)
                self.queue.next_window()
            if  row.point.timestamp() <= window_end:
                self.queue.add(row)
                # all_points.setdefault(row.mmsi, []).append(row.point)
            else:
                print(row.point.timestamp())  # to check the correctness
                self.aggregate(self.queue.flush_ends())
                break
                
        trips_dico = {key: TGeomPointSeq.from_instants(self.kept_points[key]) for key in self.kept_points}
        self.trips = pd.DataFrame.from_dict(trips_dico, orient='index', columns=["trajectory"])
        self.trips.index.names=["mmsi"]
        return # results, all_points
    
    
    def compute_trips(self):
        pass
    
    def plot_results(self):
        """To use only for debugging purposes"""
        initial_trips = self.initial_trips.trajectory.tolist()
        final_trips = self.trips.trajectory
        plot_trips(initial_trips + list(final_trips), 
                   ["b"]*len(initial_trips) + ["r"]*len(final_trips))
        

    @property
    def compressed_trips(self):
        try:
            return self.trips
        except AttributeError:
            self.compress()
            return self.trips
        
        
