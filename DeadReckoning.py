# from typing import Dict, List

from pyproj import Proj, transform

from pymeos import *
from pymeos.main.tpoint import TGeomPointInst, TGeomPointSeq

# from PriorityQueue import *

from datetime import datetime, timedelta

import pandas as pd

import numpy as np

from utility import *


class DeadReckoning():
    """
    Class performing the DeadReckoning compression.
    """
    
    def __init__(self, threshold=10,  # distance in meters
                 instants=None, 
                 trips=None,  # should be a dataframe with a column trajectory
                 proj="EPSG:25832"
                 ):

        self.threshold = threshold/2
        self.instants = instants # to adapt for case where we provide trips
        #self.compute_start_end()
        # self.initial_trips = trips if trips is not None else self.compute_trips()
        self.trips = {}
        self.last_projected_points = {}
        # self.buffered_points = {}
        self.speeds = {}
        self.lasts = {}
        self.nys = Proj(proj, preserve_units=True)
        self.nys4326 = Proj("EPSG:4326")
        self.proj_in = proj
        self.proj_out = "EPSG:4326"



    def compress(self):
        for i, row in self.instants.iterrows():
            if i % 10000 == 0:
                print(i)
            key = row["id"]
            if key not in self.trips:
                self.trips[key] = [row]
            else: 
                previous = self.trips[key][-1]
                previous_pt = Point(self.nys(previous.point.value().x, previous.point.value().y))
                previous_time = previous.point.timestamp()
                speed = previous.sog.value()*1852/3600 # from knots to m/s
                angle = ((previous.cog.value()+90)% 360)*np.pi/180  # angle degree % true north -> angle in radians
                delta = (row.point.timestamp() - previous_time).seconds 
                expected_pos = Point(previous_pt.x + delta*speed*np.cos(float(angle)),
                                     previous_pt.y + delta*speed*np.sin(float(angle)))

                current = Point(self.nys(row.point.value().x, row.point.value().y))
                distance = expected_pos.distance(current)
                # print(distance)
                if False and previous.sog.value()>0:
                    print()
                    previous_pos_deg = transform(self.proj_in, self.proj_out, previous_pt.x, previous_pt.y)
                    expected_pos_degrees = transform(self.proj_in, self.proj_out, expected_pos.x, expected_pos.y)
                    print(previous.sog.value(), previous.cog.value(), speed)
                    print("prev:", previous.point.value().x, previous.point.value().y, "->", previous_pt, "->", previous_pos_deg)
                    print("expt:", expected_pos, "->", expected_pos_degrees)
                    print("time:", row.point.timestamp(), previous_time, delta)
                    distance2 = previous_pt.distance(expected_pos)
                    print("distance from", current, distance2)
                    # print("distance from", current, distance)
                if distance > self.threshold:
                    # print(distance)
                    self.trips[key].append(row)
                    if key in self.lasts:
                        self.lasts.pop(key)
                else:
                    self.lasts[key] = row


    def finalize_trips(self):
        """Build TGeomPoint sequences from the kept points."""
        # self.isolate_single_points()  # we can not make sequences with single points ? maybe now it works since upper
        # bound inclusive
        # trips_lists ={key: [row.point for row in self.trips[key]] + [x.point for x in self.lasts.get(key, [])] for key in self.trips}
        for key,v in self.lasts.items():
            self.trips[key].append(v)

        trips_dico = {key: TGeomPointSeq.from_instants([x.point for x in traj], upper_inc=True) for key,traj in self.trips.items()}

        self.trips = pd.DataFrame.from_dict(trips_dico, orient='index', columns=["trajectory"])
        self.trips.index.names=["id"]



