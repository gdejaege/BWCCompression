
from sortedcontainers import SortedList
# from pymeos.db.psycopg2 import MobilityDB
from pymeos import *
from pymeos.main.tpoint import TGeomPointInst, TGeomPointSeq

from datetime import datetime, timedelta


from pyproj import Proj

from shapely.geometry import Point, LineString, Polygon
from shapely.ops import transform

import haversine



DEBUG = False

CURVE = 209974000
STRAIGHT = 231701000

STUDIED = {209974000: "CURVED", 231701000: "STRAIGHT"}


class PriorityPoint():
    """
    Class wrapping a point to compute its priority.
    """

    def __init__(self, mmsi, point, index=None):
        self.mmsi = mmsi
        self.point = point   # TGeomInst


    def update_priority(self, deleted_neighboor):
        self.priority = self.priority + deleted_neighboor.priority
        
        
    def compute_priority(self, before, after, synchronized=True, plot=False, traj=None):   # plot and traj only for debug
        """
        before and after are TGeomInst
        """
        # print(self.point, before.point, after.point, end="\n")
        point = self.point.value()
        line = TGeomPointSeq.from_instants([before.point, after.point])
        synchronized_point = line.value_at_timestamp(self.point.timestamp())
        

        if synchronized:
            distance = haversine.haversine((point.y, point.x), (synchronized_point.y, synchronized_point.x))*1000
        else:
            nys=Proj('EPSG:25832')
            point_proj = nys(point.x, point.y)
            # synch_proj = nys(synchronized_point.x, synchronized_point.y)
            line_proj = LineString([nys(p.value().x, p.value().y) for p in line.instants()])
            distance = Point(point_proj).distance(line_proj)
            
        self.priority = -distance
                
        # if plot and False:
        if plot and False:
            name = "straight" if self.mmsi == 231701000 else "curve"
            print(name, distance)
            plot_priority(point, line, synchronized_point, traj)

    def get_priority(self):
        return self.priority



class PriorityQueue():
    def __init__(self, mx=10):
        self.elements = SortedList(key=lambda x: x.get_priority()) # priorities!
        self.mx = mx
        self.starts = {}
        self.ends = {}
        self.trips = {}   # could be lists sorted by time !
        self.buffered_priorities = {}
        # self.startTime = startTime
        
    def pop(self):
        """Removes the element with lowest priority.
        
        Some elements should not be removed maybe ? for instance if only one point for a trip?
        """
        pass
    
    
    
    def remove_and_recompute_priority(self, to_remove: PriorityPoint):
        """First idea: if we remove a node we recompute the priority of the neighboors.
        To do this we need to remove the neighboors, recompute their priorities then add again.
        
        It seems not to work very well: the recompute priority reflects the simplified trajectory
            -> errors can propagate
        
        """
        mmsi = to_remove.mmsi
        if mmsi == CURVE:
            pass
        # find neighboor[s] of the removed elements
        trip = self.trips[mmsi]
        index_remove = trip.index(to_remove)

        neighboors = []
        if index_remove > 0:
            neighboor_before = trip[index_remove-1]      # to recompute its priority we need nodes even before
                                                         # and after
            neighboor_before_before = trip[index_remove-2] if index_remove -2 > 0 else self.starts[mmsi]
            neighboor_before_after = trip[index_remove+1] if len(trip) > index_remove +1 else self.ends[mmsi]
            neighboors.append((neighboor_before, neighboor_before_before, neighboor_before_after))

        # same for the neighboor after
        if len(trip) > index_remove + 1:
            neighboor_after = trip[index_remove+1]
            neighboor_after_before = trip[index_remove-1] if index_remove -1 > 0 else self.starts[mmsi]
            neighboor_after_after = trip[index_remove+2] if len(trip) > index_remove +2 else self.ends[mmsi]

            neighboors.append((neighboor_after, neighboor_after_before, neighboor_after_after))

        # print("removing", to_remove.point, "\n \t neighboors", neighboors[0].point, neighboors[-1].point)

        trip.pop(index_remove)

        # re-insert the neighboors in the list to update their priority
        # recompute the priority of these neighboors
        for neighboor in neighboors:
            self.elements.remove(neighboor[0])
            if mmsi == 231701000 or mmsi == 209974000:
                ppoints = [self.starts[mmsi]] + trip + [self.ends[mmsi]]
                points = [pp.point for pp in ppoints]
                traj = TGeomPointSeq.from_instants(points)
                neighboor[0]
                # neighboor[0].compute_priority(neighboor[1], neighboor[2], plot=True, traj=traj)
            else:
                neighboor[0].compute_priority(neighboor[1], neighboor[2])
            self.elements.add(neighboor[0])
    
    
    def remove_and_update_priority(self, to_remove:PriorityPoint):
        """When a node is removed, its priority is added to its neighboors."""
        mmsi = to_remove.mmsi
        if mmsi == CURVE:
            pass
        # find neighboor[s] of the removed elements
        trip = self.trips[mmsi]
        index_remove = trip.index(to_remove)

        neighboors = []
        if index_remove > 0:
            neighboors.append(trip[index_remove-1])  
        if len(trip) > index_remove + 1:
            neighboors.append(trip[index_remove+1])
        else:
            self.buffered_priorities[mmsi] += to_remove.priority

        # we need to delete, update and then re-insert in the sorted list
        for neighboor in neighboors:
            self.elements.remove(neighboor)
            neighboor.update_priority(to_remove)
            self.elements.add(neighboor)
        
        trip.pop(index_remove)
        
        return
        
    def add(self, element: PriorityPoint):
        """Adding a point to our priorty queue. This might lead to removing a point and updating priorities."""
        mmsi = element.mmsi
        
        if mmsi not in self.starts:
            self.starts[mmsi] = PriorityPoint(*element)
        elif mmsi not in self.ends:
            self.ends[mmsi] = PriorityPoint(*element)
            self.trips[mmsi] = []
            self.buffered_priorities[mmsi] = 0
        else:
            to_add = self.ends[mmsi]
            self.ends[mmsi] = PriorityPoint(*element)
            
            prior = self.trips[mmsi][-1] if len(self.trips[mmsi])>0 else self.starts[mmsi]
            
            # print(prior, self.ends[mmsi])
            to_add.compute_priority(prior, self.ends[mmsi], synchronized=False)
            to_add.priority += self.buffered_priorities[mmsi]
            self.buffered_priorities[mmsi] = 0
            
            
            self.elements.add(to_add)
            self.trips[mmsi].append(to_add)
            
            if mmsi in STUDIED and DEBUG:
                print("adding", STUDIED[mmsi])
                self.print_trajectory(mmsi)
                # print(STUDIED[mmsi], to_add.priority)
            
            if len(self.elements) + len(self.starts) > self.mx:
                to_remove = self.elements.pop()
                if to_remove.mmsi in STUDIED and DEBUG:
                    print("removing", STUDIED[to_remove.mmsi], f"{to_remove.priority:.2}")
                    self.print_trajectory(to_remove.mmsi)
                self.remove_and_update_priority(to_remove)
                if DEBUG and to_remove.mmsi in STUDIED:
                    print("removed", STUDIED[to_remove.mmsi])
                    self.print_trajectory(to_remove.mmsi)
            
        return
    

    def print_trajectory(self, mmsi: int):
        priorities = [x.priority for x in self.trips[mmsi]]
        priorities_str = [f"{x:.2f}" for x in priorities]
        string = ",".join(priorities_str) + " :: " + f"{self.buffered_priorities[mmsi]:.2f}"
        print("\t", string)

    def get_trajectories(self, end=False) -> dict:
        trajectories = {}
        for mmsi in self.starts:
            
            ppoints = [self.starts[mmsi]] + self.trips.get(mmsi, []) 
            if mmsi in self.ends and end:
                ppoints.append(self.ends[mmsi])
            trajectories[mmsi] = [pp.point for pp in ppoints]
        
        return trajectories
    
    
    def next_window(self):
        """For the next window, we keep the last points of each trajectory"""
        self.starts = self.ends
        self.elements = SortedList(key=lambda x: x.get_priority()) # priorities!
        self.ends = {}
        self.trips = {}   # could be lists sorted by time !
        self.buffered_priorities = {}
        

    def flush_ends(self):
        res = self.get_trajectories(end=True)
        return res
