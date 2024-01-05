
from sortedcontainers import SortedList
# from pymeos.db.psycopg2 import MobilityDB
from pymeos import *
from pymeos.main.tpoint import TGeomPointInst, TGeomPointSeq

from datetime import datetime, timedelta


from pyproj import Proj

from shapely.geometry import Point, LineString, Polygon
from shapely.ops import transform

import haversine
import gc



DEBUG = False

CURVE = 209974000
STRAIGHT = 231701000

STUDIED = {209974000: "CURVED", 231701000: "STRAIGHT"}


class PriorityPoint():
    """
    Class wrapping a point to compute its priority.
    """
    def __init__(self, mmsi, point):
        self.mmsi = mmsi
        self.point = point   # TGeomInst
        self.priority = 0


class PriorityQueue():
    """The simplest form: Squish's heuristic for priorities.

    When adding a point we compute the priority ass the synchronized 
    error induced in representation by its removal


    """
    def __init__(self, mx, precheck=True):
        self.priority_list = SortedList(key=lambda x: x.priority) # priorities!
        gc.enable()
        self.mx = mx
        self.starts = {}
        self.ends = {}
        self.trips = {}   # could be lists sorted by time !
        self.precheck = precheck
        self.lasts_not_added = {}
        # print("Priority Queue init")
    

    def add(self, node: PriorityPoint):
        """Adding a point to our priorty queue. This might lead to removing a point and updating priorities.
        """
        mmsi = node.mmsi
        
        if mmsi not in self.starts:
            self.starts[mmsi] = node
        elif mmsi not in self.ends:
            self.ends[mmsi] = node
            self.trips[mmsi] = []
            if mmsi in self.lasts_not_added:
                self.lasts_not_added.pop(mmsi)
        else:
            # check if node is candidate:
            first = self.trips[mmsi][-1] if len(self.trips[mmsi]) > 0 else self.starts[mmsi]
            second = self.ends[mmsi]

            # such as DR -> keep this point in memory if its the last point we need it !
            if self.precheck and len(self.priority_list) > 0 and (len(self.priority_list) + len(self.ends) >= self.mx):
                # print("precheck:", self.priority_list[0].priority)
                if self.synchronized_distance(first, second, node) < self.priority_list[0].priority:
                    self.lasts_not_added[mmsi] = node
                    return

            if mmsi in self.lasts_not_added:
                self.lasts_not_added.pop(mmsi)

            to_add = self.ends[mmsi]
            self.ends[mmsi] = node
            self.insert_node(to_add)
            
            while len(self.priority_list) + len(self.ends) > self.mx:
                # print(len(self.priority_list), len(self.starts), self.mx) 
                if len(self.priority_list) > 0:
                    to_remove = self.priority_list.pop(0)
                    self.remove_and_update_priority(to_remove)
                else:
                    # remove a random end point
                    print("random removal")
                    to_del = list(self.ends.keys())[0]
                    self.ends.pop(to_del)
        return

    def insert_node(self, to_add):
        """Compute priority and insert node."""
        mmsi = to_add.mmsi
        self.trips[mmsi].append(to_add)
        self.compute_priority(to_add)
        self.priority_list.add(to_add)

    def synchronized_distance_neighboors(self, node, synchronized=True):
        previous, following = self.find_neighboors(node) 
        return self.synchronized_distance(previous, node, following, synchronized=synchronized)


    def synchronized_distance(self, previous, node, following, synchronized=True):
        if node.point == following.point: # if same as next:
            return

        point = node.point.value()

        line = TGeomPointSeq.from_instants([previous.point, following.point])
        
        if synchronized:
            synchronized_point = line.value_at_timestamp(node.point.timestamp())
            distance = haversine.haversine((point.y, point.x), (synchronized_point.y, synchronized_point.x))*1000
        else:
            nys=Proj('EPSG:25832')
            point_proj = nys(point.x, point.y)
            # synch_proj = nys(synchronized_point.x, synchronized_point.y)
            line_proj = LineString([nys(p.value().x, p.value().y) for p in line.instants()])
            distance = Point(point_proj).distance(line_proj)
        return distance



    def compute_priority(self, node, synchronized=True, add=True):
        """Compute the priority in a squish way. """
        # we have to add in squish to be sure not to lose repported priorities
        # print("squish")
        node.priority += self.synchronized_distance_neighboors(node) 

        
    def remove_and_update_priority(self, to_remove:PriorityPoint):
        """Squish: When a node is removed, its priority is added to its neighboors.

        Its a heuristic and has the additional
        problem of favoring trips with high frequency of points.
        The priority of the removed node is added to its neighboors
        """
        mmsi = to_remove.mmsi
        trip = self.trips[mmsi]
        index_remove = trip.index(to_remove)
        trip.pop(index_remove)

        neighboors = []
        if index_remove > 0:
            neighboors.append(trip[index_remove-1])  

        if index_remove < len(trip):  
            neighboors.append(trip[index_remove])
        else:
            # in this case the next node is not yet in the priority list but
            # in the lasts
            self.ends[mmsi].priority += to_remove.priority

        # we need to delete, update and then re-insert in the sorted list
        for neighboor in neighboors:
            self.priority_list.remove(neighboor)
            neighboor.priority += to_remove.priority
            self.priority_list.add(neighboor)
        return
        
    
    def get_trajectories(self, end=False) -> dict:
        trajectories = {}
        for mmsi in self.starts:
            if not end and mmsi not in self.ends:
                continue
            ppoints = [self.starts[mmsi]] + self.trips.get(mmsi, []) 
            if mmsi in self.ends and end:
                ppoints.append(self.ends[mmsi])
            if mmsi in self.lasts_not_added and end:
                ppoints.append(self.lasts_not_added[mmsi])
            trajectories[mmsi] = [pp.point for pp in ppoints]
        
        self.next_window()
        return trajectories


    def next_window(self):
        """For the next window, we keep the last points of each trajectory.
        Even if this last point is the first one."""
        
        for mmsi in self.starts:
            if mmsi not in self.ends:
                self.ends[mmsi] = self.starts[mmsi]

        self.starts = self.ends
        self.priority_list = SortedList(key=lambda x:x.priority) # priorities!
        self.ends = {}
        self.trips = {}   # could be lists sorted by time !
        gc.collect()
        

    def flush_ends(self):
        res = self.get_trajectories(end=True)
        return res


    def find_neighboors(self, node: PriorityPoint):
        """Return the previous end following nodes in the representation."""
        mmsi = node.mmsi
        trip = self.trips[mmsi]
        index = trip.index(node)

        previous = trip[index-1] if index > 0 else self.starts[mmsi]
        
        following = trip[index+1] if index+1 < len(trip) else self.ends[mmsi]

        return previous, following



class PriorityQueueSTTrace(PriorityQueue):
    def remove_and_update_priority(self, to_remove: PriorityPoint):
        """STTrace: Remove a node we recompute the priority of the neighboors.
        To do this we need to remove the neighboors, recompute their priorities then add again.
        
        STTrace way to do: the recompute priority is based on the simplified trajectory.
        If there are many points, the error will always be small and the algorithm doesn't accumulate 
        their errors.
        """
        mmsi = to_remove.mmsi
        trip = self.trips[mmsi]
        index_remove = trip.index(to_remove)
        trip.pop(index_remove)

        neighboors = []
        if index_remove > 0:
            previous = trip[index_remove-1]      # to recompute its priority we need nodes even before
            neighboors.append(previous)

        # same for the neighboor after
        if index_remove < len(trip):
            following = trip[index_remove] # since node already removed
            neighboors.append(following) #, before_following, after_following))

        # re-insert the neighboors in the list to update their priority
        # recompute the priority of these neighboors
        for neighboor in neighboors:
            self.priority_list.remove(neighboor)
            self.compute_priority(neighboor)
            self.priority_list.add(neighboor)



    def compute_priority(self, node, synchronized=True, add=True):
        """Compute the priority in a STTrace way: distance to neighboors"""
        node.priority = self.synchronized_distance_neighboors(node) 
    

class PriorityQueueSTTraceOpt(PriorityQueue):
    def __init__(self, mx, precheck=False):
        super().__init__(mx, precheck=precheck)
        self.init_trajectories = {}


    def next_window(self):
        super().next_window()
        for mmsi, sample_start in self.starts.items():
            first = self.init_trajectories[mmsi][0]
            while first.point.timestamp() < sample_start.point.timestamp():
                self.init_trajectories[mmsi].pop(0)
                first = self.init_trajectories[mmsi][0]

    def add(self, node):
        """Adding a point to our priorty queue. This might lead to removing a point and updating priorities."""
        self.init_trajectories.setdefault(node.mmsi, []).append(node)
        super().add(node)
        return


    # def compute_priority(self, previous, current, following):
    def compute_priority(self, node):
        """The priority of a node is the real difference of errors with and without it.

        priority = Error(previous-following) - Error(previous-current-following)
        """
        def distance(instant, line):
            synchronized_point = line.value_at_timestamp(instant.timestamp())
            point = instant.value()
            return haversine.haversine((point.y, point.x), (synchronized_point.y, synchronized_point.x))*1000

        mmsi = node.mmsi
        previous, following = self.find_neighboors(node) 

        old_curve = TGeomPointSeq.from_instants([previous.point, node.point, following.point])
        new_curve = TGeomPointSeq.from_instants([previous.point, following.point])

        old_dist, new_dist = 0, 0

        traj =  self.init_trajectories[mmsi]
        node_index = traj.index(node)
        end = traj.index(following)  

        i = traj.index(previous) + 1           # the initial point has no error
        while i < end:
            point = self.init_trajectories[mmsi][i].point   # to replace with sampling regular interval !
            new_dist += distance(point, new_curve)
            old_dist += distance(point, old_curve)
            i += 1  

        node.priority = new_dist - old_dist



class PriorityQueueSTTraceOptReg(PriorityQueueSTTraceOpt):
    def __init__(self, mx, freq, precheck=False):
        super().__init__(mx, precheck=precheck)
        self.delta = timedelta(**freq)


    def compute_priority(self, node):
        """The priority of node is the real difference of error when deleting it.

        priority = Error(previous-following) - Error(previous-node-following)
        """
        def distance_instant_line(instant, line):
            synchronized_point = line.value_at_timestamp(instant.timestamp())
            point = instant.value()
            return haversine.haversine((point.y, point.x), (synchronized_point.y, synchronized_point.x))*1000

        def distance_point_line_time(point, time, line):
            """Distance between Point and the value of the line at specific time."""
            synchronized_point = line.value_at_timestamp(time)
            return haversine.haversine((point.y, point.x), (synchronized_point.y, synchronized_point.x))*1000

        previous, following = self.find_neighboors(node) 
        mmsi = node.mmsi

        old_curve = TGeomPointSeq.from_instants([previous.point, node.point, following.point])
        new_curve = TGeomPointSeq.from_instants([previous.point, following.point])

        old_error, new_error = 0, 0

        time = previous.point.timestamp() + self.delta
        end = following.point.timestamp()

        if time >= end:
            return 0


        correct_trip = TGeomPointSeq.from_instants([x.point for x in self.init_trajectories[mmsi]])
        """
        try:
             correct_trip = TGeomPointSeq.from_instants([x.point for x in self.init_trajectories[mmsi]])
        except:
             print(time, end)
             node.priority = 0
             return 
        """
        while time < end:
            correct_point = correct_trip.value_at_timestamp(time)
            new_error += distance_point_line_time(correct_point, time, new_curve)
            old_error += distance_point_line_time(correct_point, time, old_curve)
            time += self.delta
        del correct_trip

        node.priority =  new_error - old_error


