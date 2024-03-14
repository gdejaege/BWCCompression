from sortedcontainers import SortedList

from utility import *


class PriorityPoint():
    """
    Class wrapping a point to compute its priority.
    """
    def __init__(self, row):
        self.tid = row["id"]
        self.point = row["point"]   # TGeomInst
        self.priority = 0


class BWC_SQUISH():
    def __init__(self, points, window_lenght, limit, nys):
        self.instants = points # dataframe of points (can be with SOG, COG)
        self.window = window_lenght
        self.limit = limit
        self.nys = nys
        self.trips = {}  # trips # the points kept in the trips before the window
        # window related attributes
        self.priority_list = SortedList(key=lambda x: x.priority) # priorities!
        self.window_trips = {}   # could be lists sorted by time !
        self.end_priorities = {} # buffered priorities to add to last point


    def compress(self):
        """Compress all the points (in different time windows)."""
        start = self.instants.iloc[0].point.timestamp()
        window_end = start + self.window

        for i, row in self.instants.iterrows():
            time = row.point.timestamp()
            if time > window_end:
                window_end = window_end + self.window
                self.next_window()
            self.add_point(PriorityPoint(row))

        # keep points of last window
        self.next_window()
        self.finalize_trips()


    def next_window(self):
        """Empty the priorityQueue to the kept points."""
        for trip in self.window_trips:
            self.trips.setdefault(trip, []).extend(self.window_trips[trip])
            # added += len(self.window_trips[trip])

        self.priority_list = SortedList(key=lambda x: x.priority) # priorities!
        self.window_trips = {}   # could be lists sorted by time !
        # the priorities buffered at the end are valid for next window start
        # self.start_priorities = self.end_priorities
        self.end_priorities = {} 


    def add_point(self, point):
        """Process the incoming point then remove from queue and update priorities."""
        existing_len = len(self.window_trips.get(point.tid, [])) + len(self.trips.get(point.tid, []))
        if existing_len > 0:
            point.priority = 1e20
        else:
            point.priority = float('inf')
        self.priority_list.add(point)
        self.window_trips.setdefault(point.tid, []).append(point)

        if len(self.window_trips[point.tid]) > 1:
            self.update_priority_antelast_point(point.tid)

        while len(self.priority_list) > self.limit:
            self.remove_point()


    def update_priority_antelast_point(self, tid):
        """Compute the priority (SED) of point before the new last one of trajectory."""
        trip = self.window_trips[tid]
        if tid not in self.trips and len(trip) == 2:
            # the antelast is the first, therefore we keep priority infinite
            return

        to_update = trip[-2] # the window_trip size already been checked
        self.priority_list.remove(to_update) 
        to_update.priority = self.evaluate_point(to_update) 

        # if there is a buffered priorities at the end, we add it
        # For instance if we have the trajectory "a b c d" to which e will be added,
        # if for some reason c was dropped before the addition of e, we add the 
        # priority of c to d.
        if tid in self.end_priorities:
            to_update.priority += self.end_priorities.pop(tid)

        self.priority_list.add(to_update)
        return
        

    def remove_point(self):
        """Remove point with least priority and update its neighboors' priorities."""
        to_remove = self.priority_list.pop(0)
        tid = to_remove.tid
        trip = self.window_trips[tid]
        to_remove_index = trip.index(to_remove)
        del trip[to_remove_index]

        # update priority of the neighboors using SQUISH heuristic
        if to_remove_index > 0:
            previous = trip[to_remove_index - 1]
            self.priority_list.remove(previous)
            previous.priority += to_remove.priority
            self.priority_list.add(previous)

        if to_remove_index < len(trip):
            following = trip[to_remove_index] # since node already removed
            self.priority_list.remove(following)
            following.priority += to_remove.priority
            self.priority_list.add(following)
        else:
            # if there is no following we buffer the priority:
            self.end_priorities[tid] = self.end_priorities.get(tid, 0) + to_remove.priority


    def evaluate_point(self, point):
        """returns the original SED evaluation."""
        tid = point.tid
        full_trip = self.trips.get(tid, [])[-1:] + self.window_trips[tid]
        point_id = full_trip.index(point)

        # normally it should not happen because in squish the evaluation is done only at first insertion
        # after, the updating is done by adding the deleted point priority
        if point_id == 0 or point_id == len(full_trip):
            # print("error")
            return float('inf')
        else:
            return compute_SED(full_trip[point_id-1].point, point.point, 
                               full_trip[point_id+1].point, self.nys)
        

    def finalize_trips(self):
        """Build TGeomPoint sequences from the kept points."""
        trips_dico = {key: TGeomPointSeq.from_instants([x.point for x in traj], upper_inc=True) for key, traj in self.trips.items()}

        # 
        self.trips = pd.DataFrame.from_dict(trips_dico, orient='index', columns=["trajectory"])



def classical_squish(trips, ratio, delta, nys):
    """Same but with only 1 time window."""
    res = {}
    for mmsi, row in trips.iterrows():
        trajectory = row.trajectory
        nb_points = max(len(trajectory.instants())//ratio, 3)
        points = convert_trajectory_points(mmsi, trajectory)

        bwc_squish = BWC_SQUISH(points, window_lenght=delta, 
                                           limit=nb_points, 
                                           nys=nys)
        bwc_squish.compress()
        res_mmsi = bwc_squish.trips
        
        for mmsi, row in res_mmsi.iterrows():
            res[mmsi] = row.trajectory
        
    results = pd.DataFrame.from_records(((mmsi, trajectory) for mmsi, trajectory in res.items()), 
                                        columns=["id", "trajectory"], index="id")
    return results


def assess_bwc_squish(points, window_lenght, limit, nys):
    """For tests only."""
    squish = BWC_SQUISH(points, window_lenght, limit, nys)

    squish.compress()
    squish.finalize_trips()
    trips = squish.res
    t = 0
    for i, trip in trips.iterrows():
        x = len(trip.trajectory.instants()) 
        print(t, i, x)
        t += 1
    print(squish.res.head())
    print("squish finished")




if __name__ == "__main__":
    import DBHandler
    from configobj import ConfigObj
    from pymeos import *
    from datetime import datetime, timedelta
    from pyproj import Proj

    test = "copenhague_15min"

    pymeos_initialize()

    CONFIG = ConfigObj("test_config.ini")
    CONFIG_DATA = CONFIG[CONFIG[test]["dataset"]]
    CONFIG = CONFIG[test]

    delta = {CONFIG["WINDOW_SIZE_UNIT"]: CONFIG.as_int("WINDOW_SIZE")}
    CONFIG["WINDOW_LENGTH"] = timedelta(**delta)

    dbhandler = DBHandler.DBHandler(db=CONFIG_DATA["db"], debug=False)
    trips = dbhandler.load_table(table="trips_cleaned", columns=["id", "trajectory"], df_index="id")
    points = dbhandler.load_table(table="points_cleaned", columns=CONFIG_DATA["points_columns"])
    points = points.sort_values(by=['point'], ascending=True)
    dbhandler.close()

    nys = Proj(CONFIG_DATA["proj"], preserve_units=True)

    assess_bwc_squish(points=points, window_lenght=CONFIG["WINDOW_LENGTH"], 
                      limit=CONFIG.as_int("NPOINTS"), 
                      nys=nys)



