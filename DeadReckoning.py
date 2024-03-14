# from typing import Dict, List

from pyproj import Proj, transform

from pymeos import *
from pymeos.main.tpoint import TGeomPointInst, TGeomPointSeq

from sortedcontainers import SortedList

from datetime import datetime, timedelta

import pandas as pd

import numpy as np

from utility import *



class DeadReckoning():
    """
    Class performing the DeadReckoning compression.
    """
    
    def __init__(self, threshold,  # distance in meters
                 instants, 
                 # trips=None,  # should be a dataframe with a column trajectory
                 nys # 25832"
                 ):

        self.threshold = threshold/2
        self.instants = instants # to adapt for case where we provide trips
        self.trips = {}
        self.lasts = {}
        self.nys = nys


    def get_expected_pos_sog(self, time, previous):
        previous_pt = Point(self.nys(previous.point.value().x, previous.point.value().y))
        previous_time = previous.point.timestamp()

        speed = previous.sog*1852/3600 # from knots to m/s
        angle = (previous.cog% 360)*np.pi/180  # angle degree % true north -> angle in radians
        delta = (time - previous_time).seconds 

        expected_pos = Point(previous_pt.x + delta*speed*np.sin(float(angle)),
                             previous_pt.y + delta*speed*np.cos(float(angle)))

        return expected_pos 


    def get_expected_pos(self, time, key):
        # if we compute expected pos, there must be a previous one
        trip = self.trips[key]
        previous = trip[-1]
        if hasattr(previous, "sog"):
            return self.get_expected_pos_sog(time, previous)
        # if we don't know the SOG, COG we must compute it with anteprev
        if len(trip) <= 1:
            return Point(*self.nys(previous.point.value().x, previous.point.value().y))
        else:
            return self.expected_pos_anteprev(time, previous, trip[-2])
        

    def expected_pos_anteprev(self, time, previous, anteprev):
        prev_pt = Point(self.nys(previous.point.value().x, previous.point.value().y))
        anteprev_pt = Point(self.nys(anteprev.point.value().x, anteprev.point.value().y))
        dt = (previous.point.timestamp() - anteprev.point.timestamp()).total_seconds()
        vx, vy = (prev_pt.x - anteprev_pt.x)/dt, (prev_pt.y - anteprev_pt.y)/dt
        new_dt = (time - previous.point.timestamp()).total_seconds()
        return Point(prev_pt.x + vx*new_dt,  prev_pt.y + vy*new_dt)  



    def compress(self):
        for i, row in self.instants.iterrows():
            if i % 10000 == 0:
                print(i//10000, end=", ")

            key = row["id"]
            if key not in self.trips:
                self.trips[key] = [row]
            else: 
                expected_pos = self.get_expected_pos(time=row.point.timestamp(), key=key)
                current = Point(self.nys(row.point.value().x, row.point.value().y))
                distance = expected_pos.distance(current)

                if distance > self.threshold and key in self.lasts:
                    # we add the last point before exceeding the threshold
                    last = self.lasts.pop(key)
                    self.trips[key].append(last)
                    expected_pos = self.get_expected_pos(time=row.point.timestamp(), 
                                                         key=key)
                    distance = expected_pos.distance(current)

                # this is not an elif as the distance is recomputed with insertion of the last point
                if distance > self.threshold:
                    self.trips[key].append(row)
                    if key in self.lasts:
                        # self.trips[key].append(last)
                        self.lasts.pop(key)
                else:
                    self.lasts[key] = row


    def finalize_trips(self, include_last=True):
        """Build TGeomPoint sequences from the kept points."""
        # self.isolate_single_points()  # we can not make sequences with single points ? maybe now it works since upper
        # bound inclusive
        # trips_lists ={key: [row.point for row in self.trips[key]] + [x.point for x in self.lasts.get(key, [])] for key in self.trips}
        if include_last:
            for key,v in self.lasts.items():
                self.trips[key].append(v)

        # check duplicates:
        for key,traj in self.trips.items():
            i = 0
            flag = False
            for i in range(len(traj)-1):
                if traj[i].point.timestamp() >= traj[i+1].point.timestamp():
                    # print("Duplicate in finalization")
                    # print(traj[i].point)
                    # print(traj[i+1].point)
                    flag = True
            if flag:
                print(traj)
                print("Above")


        trips_dico = {key: TGeomPointSeq.from_instants([x.point for x in traj], upper_inc=True) for key,traj in self.trips.items()}

        self.trips = pd.DataFrame.from_dict(trips_dico, orient='index', columns=["trajectory"])
        self.trips.index.names=["id"]



if __name__ == "__main__":
    import DBHandler
    from configobj import ConfigObj
    test = "copenhague_5min"
    test = "birds_3months"
    print(test)

    pymeos_initialize()
    CONFIG = ConfigObj("tests.ini")
    CONFIG = CONFIG[test]

    delta = {CONFIG["compression"]["WINDOW_SIZE_UNIT"]: CONFIG["compression"].as_int("WINDOW_SIZE")}
    CONFIG["compression"]["WINDOW_LENGTH"] = timedelta(**delta)

    dbhandler = DBHandler.DBHandler(db=CONFIG["db"], debug=False)
    trips = dbhandler.load_table(table="trips_cleaned", columns=["id", "trajectory"], df_index="id")
    points = dbhandler.load_table(table="points_cleaned", columns=CONFIG["points_columns"])
    points = points.sort_values(by=['point'], ascending=True)
    dbhandler.close()


    dr = DeadReckoning(instants=points, threshold=5)

    dr.compress()
    dr.finalize_trips(include_last=False)
    print("BWCDR finished")


