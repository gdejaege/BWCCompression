from datetime import datetime, timedelta

from pyproj import Proj, transform
import numpy as np
import pandas as pd
import pymeos

# from sortedcontainers import SortedList

from src.helpers.utility import Instant, get_expected_pos, compute_distance

#### THIS METHOD IS NOT INCLUDED IN THE PAPER

WGS84 = Proj(init='EPSG:4326')

class BWC_DR_RT:

    def __init__(self, points, process_time: timedelta, proj) -> None:
        self.proj = proj
        self.points = points
        self.queue = []
        self.process_time = process_time # minimal time between 2 points sent
        self.last_points = {}  # last point sent per trip
        self.trajectories = {}  # compressed trajectories
        self.delays = []  # to compute RT metrics
        self.all_delays = []  # to compute RT metrics
        self.distances = [] # to compute threshold
        self.next_send_time: datetime
        self.threshold = 0
        self.basic_perc = 20
        self.lasts = {}

    def init_time(self):
        if self.points is None:
            return
        self.next_send_time = self.points.iloc[0]["point"].timestamp()

    

    def compress(self):
        self.init_time()
        print_progress = self.print_progress()
        for it, row in self.points.iterrows():
            next(print_progress)
            instant = Instant(row)
            tid = instant.tid
            self.proccess_queue(instant.point.timestamp())

            if instant.tid not in self.trajectories:
                self.keep(instant, instant.point.timestamp())
            else:
                self.update_threshold()
                distance = self.compute_distance(instant)
                self.distances.append(distance)
                if distance > self.threshold and tid in self.lasts:
                    last = self.lasts.pop(tid)
                    self.keep(last, instant.point.timestamp())
                    distance = self.compute_distance(instant)
                    self.distances.append(distance)

                # we recomute the distance to check if the processed point also exceed
                if self.threshold <= distance:
                    self.keep(instant, instant.point.timestamp())

        self.finalize_trajectories()
        print()


    def print_progress(self):
        n = len(self.points)
        t = 0.1
        it = 0
        while True:
            if it/n > t:
                print(int(t*10+0.01), end=" ", flush=True)
                # print(len(gc.garbage))
                t += 0.1
                # # try to reduce memory impact:
                # print("test finalizing")
                # pymeos.pymeos_finalize()
                # print("pymeos closed")
                # pymeos.pymeos_initialize()
                # print("test passed")
            it += 1
            yield


    def proccess_queue(self, point_time):
        if self.next_send_time <= point_time and len(self.queue) == 0:
            # we could send points but the queue is empty
            self.reduce_basic_threshold((point_time-self.next_send_time))

        # checking if points in the buFfer where sent in the meantime
        while self.next_send_time <= point_time and self.queue:
            self.send_point(self.queue.pop(0))
            
    def reduce_basic_threshold(self, timedelta):
        # if the queue is empty, this might happend for all incoming pionts until one
        # is put in the queue.
        waisted_oportunities = int(timedelta/self.process_time) + 1
        self.basic_perc = max(0, self.basic_perc - waisted_oportunities)

    def increase_basic_threshold(self):
        if 10*len(self.queue) > 50:
            self.basic_perc = min(100, self.basic_perc + 1)


    def update_threshold(self):
        if len(self.distances) == 0:
            self.threshold = 0
        else:
            perc = min(100, self.basic_perc + 10*len(self.queue))
            self.threshold = np.percentile(self.distances[-100:], perc)

    def compute_distance(self, instant: Instant):
        trip = self.trajectories[instant.tid]
        time = instant.point.timestamp()
        projected_expected_pos = get_expected_pos(trip, time, self.proj)
        # expected_pos_wgs = Point(transform(self.proj, WGS84, expected_pos.x, expected_pos.y))
        #print(expected_pos_wgs, instant.point.value())
        return compute_distance(projected_expected_pos, 
                                instant.point.value(), 
                                proj=self.proj,
                                projected=(True, False))

    def keep(self, instant, ts):
        self.trajectories.setdefault(instant.tid, []).append(instant)
        # self.last_points[instant.tid] = instant
        self.delays.append(ts - instant.point.timestamp())
        self.queue.append(instant)
        self.increase_basic_threshold()

    def send_point(self, instant):
        # maybe the point arrived later than the minimal waiting time
        sending_time = max(self.next_send_time, instant.point.timestamp())
        self.next_send_time = sending_time + self.process_time
        self.delays.append(sending_time - instant.point.timestamp())


    def finalize_trajectories(self):
        trips_dico = {
            key: pymeos.TGeomPointSeq.from_instants(
                [x.point for x in traj], upper_inc=True
            )
            for key, traj in self.trajectories.items()
        }

        self.finalized_trips = pd.DataFrame.from_dict(
            trips_dico, orient="index", columns=["trajectory"]
        )
