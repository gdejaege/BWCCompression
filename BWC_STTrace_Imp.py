from sortedcontainers import SortedList
from utility import *
from pymeos import *
from pymeos.main.tpoint import TGeomPointInst, TGeomPointSeq
import haversine

class PriorityPoint():
    """
    Class wrapping a point to compute its priority.
    """
    def __init__(self, row):
        self.tid = row["id"]
        self.point = row["point"]   # TGeomInst
        self.priority = 0


class BWC_STTrace_Imp():
    def __init__(self, points, window_lenght, limit, nys, eval_delta, init_trips):
        self.instants = points # dataframe of points (can be with SOG, COG)
        self.window = window_lenght
        self.limit = limit
        self.nys = nys
        self.trips = {}  # trips # the points kept in the trips before the window
        self.init_trips = init_trips
        # window related attributes
        self.priority_list = SortedList(key=lambda x: x.priority) # priorities!
        self.window_trips = {}   # could be lists sorted by time !
        self.eval_delta = eval_delta


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
        """Process the incoming point then remove from queue and update priorities."""
        added = 0
        for trip in self.window_trips:
            self.trips.setdefault(trip, []).extend(self.window_trips[trip])
            added += len(self.window_trips[trip])

        self.priority_list = SortedList(key=lambda x: x.priority) # priorities!
        self.window_trips = {}   # could be lists sorted by time !
        # the priorities buffered at the end are valid for next window start


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


    def interesting(self, point):
        """STTrace: dont consider points if leads to removal of current last in trip."""
        tid = point.tid
        full_trip = self.trips.get(tid,[]) + self.window_trips.get(tid, []) 

        return (len(full_trip) < 2) or compute_SED(full_trip[-2].point, full_trip[-1].point, point.point) > self.priority_list[0].priority


    def update_priority_antelast_point(self, tid):
        """Compute the priority (SED) of point before the new last one of trajectory."""
        trip = self.window_trips[tid]
        if tid not in self.trips and len(trip) == 2:
            # the antelast is the first, therefore we keep priority infinite
            return

        to_update = trip[-2] # the window_trip size already been checked
        self.priority_list.remove(to_update) 
        to_update.priority = self.evaluate_point(to_update) 
        self.priority_list.add(to_update)
        return
        

    def remove_point(self):
        """Remove point with least priority and update its neighboors' priorities."""
        to_remove = self.priority_list.pop(0)
        tid = to_remove.tid

        trip = self.window_trips[tid]

        to_remove_index = trip.index(to_remove)
        del trip[to_remove_index]

        # update priority of the neighboors
        if to_remove_index > 0:
            previous = trip[to_remove_index - 1]
            self.priority_list.remove(previous)
            previous.priority = self.evaluate_point(previous)
            self.priority_list.add(previous)

        if to_remove_index < len(trip):
            following = trip[to_remove_index]
            self.priority_list.remove(following)
            following.priority = self.evaluate_point(following)
            self.priority_list.add(following)


    def evaluate_point(self, point):
        """returns the original SED evaluation."""
        def distance_instant_line(instant, line):
            synchronized_point = line.value_at_timestamp(instant.timestamp())
            point = instant.value()
            return haversine.haversine((point.y, point.x), (synchronized_point.y, synchronized_point.x))*1000

        def distance_point_line_time(point, time, line):
            """Distance between Point and the value of the line at specific time."""
            synchronized_point = line.value_at_timestamp(time)
            return haversine.haversine((point.y, point.x), (synchronized_point.y, synchronized_point.x))*1000

        tid = point.tid
        extended_trip = self.trips.get(tid, [])[-1:] + self.window_trips[tid]
        point_id = extended_trip.index(point)

        # normally it should not happen
        if point_id == 0 or point_id == len(extended_trip) - 1:
            return float('inf')

        previous, following = extended_trip[point_id-1].point, extended_trip[point_id+1].point

        old_curve = TGeomPointSeq.from_instants([previous, point.point, following])
        new_curve = TGeomPointSeq.from_instants([previous, following])

        old_error, new_error = 0, 0

        time = previous.timestamp() + self.eval_delta
        end = following.timestamp()

        if time >= end:
            return 0

        correct_trip = self.init_trips.loc[tid].trajectory

        while time < end:
            correct_point = correct_trip.value_at_timestamp(time)
            new_error += distance_point_line_time(correct_point, time, new_curve)
            old_error += distance_point_line_time(correct_point, time, old_curve)
            time += self.eval_delta
        return new_error - old_error



    def finalize_trips(self):
        """Build TGeomPoint sequences from the kept points."""
        # check for errors
        for key, points in self.trips.items():
            i = 0
            flag = False
            for i in range(len(points)-1):
                if points[i].point.timestamp() >= points[i+1].point.timestamp():
                    flag = True
            if flag or len(points) == 0:
                print(key, points)
                print("Above")
        # traj is a list of PriorityPoints
        trips_dico = {key: TGeomPointSeq.from_instants([x.point for x in traj], upper_inc=True) for key, traj in self.trips.items()}

        # 
        self.trips = pd.DataFrame.from_dict(trips_dico, orient='index', columns=["trajectory"])


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





