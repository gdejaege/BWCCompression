from src.bwc.squish import BWC_SQUISH
from src.bwc.sttrace import BWC_STTrace
from src.helpers.utility import (
    convert_points_trips,
    convert_trip_points,
    convert_mpd_PyMeos,
    convert_PyMeos_mpd,
    get_expected_pos,
    Instant,
)
from src.helpers.utility import compute_distance as u_compute_distance

import pandas as pd
import movingpandas as mpd
import pymeos

#from pyproj import Proj
from shapely.geometry import Point


def classical_squish(points, ratio, proj):
    """Same but with only 1 time window."""
    res = {}
    trips = convert_points_trips(points)
    start = points.iloc[0].point.timestamp()
    stop = points.iloc[-1].point.timestamp()
    duration = stop - start

    for mmsi, row in trips.iterrows():
        trajectory = row.trajectory
        nb_points = max(len(trajectory.instants()) // ratio, 3)
        points = convert_trip_points(mmsi, trajectory)

        bwc_squish = BWC_SQUISH(
            points, window_lenght=duration, limit=nb_points, proj=proj
        )
        bwc_squish.compress()
        res_mmsi = bwc_squish.finalized_trips

        for mmsi, row in res_mmsi.iterrows():
            res[mmsi] = row.trajectory

    results = pd.DataFrame.from_records(
        ((mmsi, trajectory) for mmsi, trajectory in res.items()),
        columns=["id", "trajectory"],
        index="id",
    )
    return results


def classical_STTrace(points, npoints, proj):
    """Same but with 1 time window."""
    start = points.iloc[0].point.timestamp()
    stop = points.iloc[-1].point.timestamp()
    duration = stop - start
    bwc_sttrace = BWC_STTrace(points, window_lenght=duration, limit=npoints, proj=proj)
    bwc_sttrace.compress()
    return bwc_sttrace.finalized_trips


def compress_trips_top_down_time_ratio(points, tolerence=100):
    """Should be adapted to use Pymeos instead."""

    def compress_mpd_synchronized_DP(mpd_trips, tolerence):
        """Compress trips in the mpd format using mpd using top down time ratio algorithm."""
        generalizing_fct = lambda trip: mpd.TopDownTimeRatioGeneralizer(
            trip.trajectory
        ).generalize(tolerance=tolerence)
        mpd_trips["trajectory"] = mpd_trips.apply(generalizing_fct, axis=1)
        return mpd_trips

    trips = convert_points_trips(points)
    mpd_trips = convert_PyMeos_mpd(trips)
    mpd_compressed = compress_mpd_synchronized_DP(mpd_trips, tolerence=tolerence)
    return convert_mpd_PyMeos(mpd_compressed)


def DeadReckoning(instants, threshold, proj):  # distance in meters  # 25832"
    def compute_distance(trip, instant, proj):
        # trip = trips[instant.tid]
        time = instant.point.timestamp()
        projected_expected_pos = get_expected_pos(trip, time, proj)
        projected_pos = Point(proj(instant.point.value().x, instant.point.value().y))
        return projected_expected_pos.distance(projected_pos)

        # return u_compute_distance(
        #     projected_expected_pos,
        #     instant.point.value(),
        #     proj=proj,
        #     projected=(True, False),
        # )

    def finalize_trajectories(trips):
        trips_dico = {
            key: pymeos.TGeomPointSeq.from_instants(
                [x.point for x in traj], upper_inc=True
            )
            for key, traj in trips.items()
        }

        finalized_trips = pd.DataFrame.from_dict(
            trips_dico, orient="index", columns=["trajectory"]
        )
        return finalized_trips
    threshold = threshold / 2
    instants = instants  # to adapt for case where we provide trips
    trips = {}
    lasts = {}
    proj = proj

    for i, row in instants.iterrows():
        if i % 10000 == 0:
            print(i // 10000, end=", ")

        instant = Instant(row)
        tid = instant.tid

        if tid not in trips:
            trips[tid] = [instant]
        else:
            distance = compute_distance(trips[tid], instant, proj)

            if distance > threshold and tid in lasts:
                last = lasts.pop(tid)
                trips[tid].append(last)
                # we recomute the distance to check if the processed point also exceed
                distance = compute_distance(trips[tid], instant, proj)

            if distance > threshold:
                trips[tid].append(instant)
                if tid in lasts:
                    lasts.pop(tid)
            else:
                lasts[tid] = instant


    return finalize_trajectories(trips)

