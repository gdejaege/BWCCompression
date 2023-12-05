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
    
    def __init__(self, 
                 window_size=timedelta(minutes=15), 
                 nb_points=25, 
                 instants=None, 
                 trips=None  # should be a dataframe with a column trajectory
                 ):

        self.window_size = window_size
        self.nb_points = nb_points
        self.instants = instants # to adapt for case where we provide trips
        self.compute_start_end()
        self.initial_trips = trips if trips is not None else self.compute_trips()
        
 

    def compute_start_end(self):
        self.start, self.end = self.instants.iloc[0].point.timestamp(), self.instants.iloc[-1].point.timestamp()
        
    
    def aggregate(self, increment:Dict[int, List[TGeomPointInst]]):
        for trajectory in increment:
            if trajectory in self.kept_points:
                self.kept_points[trajectory] += increment[trajectory]
            else:
                self.kept_points[trajectory] = increment[trajectory]

    def init_compression(self, strategy, freq):
        self.trips = {}         # Dict[int, TGeomPointSeq 
        self.kept_points = {}   # Dict[int, List[TGeomPointInst]] 
        if strategy == "SQUISH":
            self.queue = PriorityQueue(mx=self.nb_points, precheck=False)
        elif strategy == "STTrace": 
            self.queue = PriorityQueueSTTrace(mx=self.nb_points)
        elif strategy == "STTraceOpt": 
            self.queue = PriorityQueueSTTraceOpt(mx=self.nb_points, precheck=False)
        elif strategy == "STTraceOptReg": 
            self.queue = PriorityQueueSTTraceOptReg(mx=self.nb_points, freq=freq, precheck=False)
        self.window_start = self.start
        self.window_end = self.start + self.window_size


    def next_window(self, verbose=True):
        results_window = self.queue.get_trajectories()     # dico[mmsi: list(TGeomPoint)]
        if verbose:
            qty = sum([len(x) for x in results_window.values()])
            print("window:", self.window_start, self.window_end, qty)

        # self.queue.next_window()

        self.aggregate(results_window)

        self.window_start += self.window_size
        self.window_end = min(self.window_end+self.window_size, self.end)


    def compress(self, strategy="SQUISH", freq=None):
        self.init_compression(strategy, freq)
        for key, row in self.instants.iterrows():
            # print(row.point.timestamp())
            if row.point.timestamp() < self.window_start:   # should not happen here: start defined from points
                continue  

            while self.window_end < row.point.timestamp() <= self.end:   # window shifting
                self.next_window() 
                #  results_window = self.queue.get_trajectories()     # dico[mmsi: list(TGeomPoint)]

                #  qty = sum([len(x) for x in results_window.values()])
                # print("window:", self.window_start, self.window_end, qty)

                #  self.aggregate(results_window)
                #  window_start += self.window_size
                #  window_end = min(window_end+self.window_size, self.end)
                #  self.queue.next_window()

            if  row.point.timestamp() <= self.window_end:
                self.queue.add(PriorityPoint(row.id, row.point))
                # all_points.setdefault(row.mmsi, []).append(row.point)
            else:
                print(row.point.timestamp())  # to check the correctness
                break

        results_window = self.queue.flush_ends()
        qty = sum([len(x) for x in results_window.values()])
        print("window:", self.window_start, self.window_end, qty)
        self.aggregate(results_window)
        self.finalize_trips()


    def finalize_trips(self):
        """Build TGeomPoint sequences from the kept points."""
        # self.isolate_single_points()  # we can not make sequences with single points ? maybe now it works since upper
        # bound inclusive
        trips_dico = {key: TGeomPointSeq.from_instants(self.kept_points[key], upper_inc=True) for key in self.kept_points}
        self.trips = pd.DataFrame.from_dict(trips_dico, orient='index', columns=["trajectory"])
        self.trips.index.names=["mmsi"]


    def isolate_single_points(self):
        single_points = {k:v for k,v in self.kept_points.items() if len(v) == 1}
        for k in single_points:
            self.kept_points.pop(k)


    def plot_results(self):
        """To use only for debugging purposes"""
        initial_trips = self.initial_trips.trajectory.tolist()

        final_trips = self.trips.trajectory
        plot_trips(initial_trips + list(final_trips), 
                   ["b"]*len(initial_trips) + ["r"]*len(final_trips))


    def compression_stats(self, full=True):
        """Indicate the numbers of points removed."""
        total, total_init = 0, 0
        for mmsi, instants in self.kept_points.items():
            n_init_instants = len(self.instants[self.instants.mmsi == mmsi])
            total_init += n_init_instants
            total += len(instants)
            print(mmsi, n_init_instants, len(instants))

        print(total_init, "->", total)
        print(total_init/total)


    @property
    def compressed_trips(self):
        try:
            return self.trips
        except AttributeError:
            self.compress()
            return self.trips
        
        
