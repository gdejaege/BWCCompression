from shapely.geometry import Point, LineString
import numpy as np
import haversine
import pandas as pd

from pyproj import Proj

from pymeos import TGeomPointSeq


class PriorityPoint:
    """
    Class wrapping a point to compute its priority.
    """

    def __init__(self, row):
        self.tid = row["id"]
        self.point = row["point"]  # TGeomInst
        self.priority = 0
        if hasattr(row, "sog"):
            self.sog = row["sog"]
            self.cog = row["cog"]


def extract_wkt_from_traj(traj):
    """Create a wkt representation from the movingPandas trip."""
    res = "["
    for row in traj.df.iloc():
        item = row.geometry.wkt + "@" + row.name.__str__()
        res += item + ","
    res = res[:-1] + "]"
    return res


def convert_points_trips(points):
    def detect_short_trips(points):
        small_trips = points.groupby("id").size()
        return small_trips.loc[small_trips <= 1]

    trips = (
        points.loc[~points["id"].isin(detect_short_trips(points).index)]
        .groupby("id")
        .aggregate({"point": lambda x: TGeomPointSeq.from_instants(x, upper_inc=True)})
        .rename({"point": "trajectory"}, axis=1)
    )

    return trips


def convert_trips_points(trip_id, trajectory, sort=True):
    """Convert a single trajectory into a dataframe of points.

    Not adapted if there is the SOG, COG or other information
    """
    instants = trajectory.instants()

    df = pd.DataFrame.from_records(
        ((trip_id, inst) for inst in instants), columns=["id", "point"]
    )
    return df


##########################################################################
######                     Computing distances                     #######
##########################################################################


def get_expected_pos_sog(start, time, nys):
    """For AIS DATA. To adapt if other datasources."""
    start_time = start.point.timestamp()
    start_pt = Point(nys(start.point.value().x, start.point.value().y))

    speed = start.sog * 1852 / 3600  # from knots to m/s
    angle = (
        ((start.cog) % 360) * np.pi / 180
    )  # angle degree % true north -> angle in radians
    delta = (time - start_time).seconds

    expected_pos = Point(
        start_pt.x + delta * speed * np.sin(float(angle)),
        start_pt.y + delta * speed * np.cos(float(angle)),
    )

    return expected_pos


def get_expected_pos_anteprev(time, prev, anteprev, nys):
    prev_pt = Point(nys(prev.point.value().x, prev.point.value().y))
    anteprev_pt = Point(nys(anteprev.point.value().x, anteprev.point.value().y))
    dt = (prev.point.timestamp() - anteprev.point.timestamp()).total_seconds()
    vx, vy = (prev_pt.x - anteprev_pt.x) / dt, (prev_pt.y - anteprev_pt.y) / dt
    new_dt = (time - prev.point.timestamp()).total_seconds()
    return Point(prev_pt.x + vx * new_dt, prev_pt.y + vy * new_dt)


def compute_SED(A, B, C, nys, synchronized=True):
    """Return the distance of point B to segment AC."""
    # I should raise error if out of order 
    if B == C or A == B:
        return B

    point = B.value()

    line = TGeomPointSeq.from_instants([A, C])

    if synchronized:
        synchronized_point = line.value_at_timestamp(B.timestamp())
        distance = (
            haversine.haversine(
                (point.y, point.x), (synchronized_point.y, synchronized_point.x)
            )
            * 1000
        )
    else:
        # nys=Proj('EPSG:25832')
        point_proj = nys(point.x, point.y)
        line_proj = LineString([nys(p.value().x, p.value().y) for p in line.instants()])
        distance = Point(point_proj).distance(line_proj)
    return distance


def compute_distance(A, B, crs):
    """Computes the distance between two points in meters."""
    nys = Proj(crs)
    projA = Point(nys(A.x, A.y))
    projB = Point(nys(B.x, B.y))
    return projA.distance(projB)


##########################################################################
######                Assessing Compression Techniques             #######
##########################################################################


def assess_single_trajectory(compressed, original, delta, crs="EPSG:25832"):
    """The score will be the average distance of at regular interval of original trip to the compressed trajectory."""
    score = 0
    nmbr_instants = 0
    compressed_start = compressed.start_instant().timestamp()
    compressed_end = compressed.end_instant().timestamp()
    mx_distance = 0

    time = compressed_start + delta

    while time < compressed_end:
        point = original.value_at_timestamp(time)

        point_compressed = compressed.value_at_timestamp(time)
        distance = (
            haversine.haversine(
                (point.y, point.x), (point_compressed.y, point_compressed.x)
            )
            * 1000
        )
        # distance = compute_distance(point, point_compressed, crs)

        if distance > mx_distance:
            mx_distance = distance
        score += distance
        nmbr_instants += 1
        time += delta

    return score, nmbr_instants, mx_distance


def assess_single_trajectory_instants(compressed, original):
    """The score will be the average distance of each point in the original trip to the compressed trajectory."""
    score = 0
    nmbr_instants = 0
    compressed_start = compressed.start_instant().timestamp()
    compressed_end = compressed.end_instant().timestamp()
    mx_distance = 0

    for instant in original.instants():
        point = instant.value()
        timestamp = instant.timestamp()

        if not (compressed_start <= timestamp <= compressed_end):
            print("compressed trajectory shorter in time ....")

        if compressed is None:
            print("error compressed is None")

        point_compressed = compressed.value_at_timestamp(timestamp)

        distance = (
            haversine.haversine(
                (point.y, point.x), (point_compressed.y, point_compressed.x)
            )
            * 1000
        )
        if distance > mx_distance:
            mx_distance = distance
        score += distance

        nmbr_instants += 1

        # print(nmbr_instants, distance, score)

    return score, nmbr_instants, mx_distance


def assess_algorithms(trips, algorithms, original_column, precision):
    trip_ids = trips.index.values.tolist()

    scores = {algorithm: 0 for algorithm in algorithms}
    total_points = {algorithm: 0 for algorithm in algorithms}
    mx_distances = {algorithm: [] for algorithm in algorithms}

    for algorithm in algorithms:
        # print()
        # print(algorithm, end=": ")
        for i, trip_id in enumerate(trip_ids):
            if i % 10 == 0:
                # print(i, end=" ")
                continue
            score, points, dist = assess_single_trajectory(
                original=trips.loc[trip_id][original_column],
                compressed=trips.loc[trip_id][algorithm],
                delta=precision,
            )
            scores[algorithm] += score
            total_points[algorithm] += points
            mx_distances[algorithm].append(dist)

    for algorithm, s in scores.items():
        scores[algorithm] = s / total_points[algorithm]

    return scores, mx_distances


def compile_trips(results, original_trips):
    """Results should be dico[name]:TGeomPointSequence"""

    all_compressed_trajectories = original_trips.rename(
        {"trajectory": "Original"}, axis=1
    )

    for name, trajectory in results.items():
        # print(name, type(trajectory))
        all_compressed_trajectories = all_compressed_trajectories.join(
            # trajectory, how="outer"
            trajectory,
            how="inner",
        )
        all_compressed_trajectories = all_compressed_trajectories.rename(
            {"trajectory": name}, axis=1
        )

    return all_compressed_trajectories
