import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D

from pymeos import *
from pymeos.main.tpoint import TGeomPointInst, TGeomPointSeq

import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame, read_file
import movingpandas as mpd

import shapely as shp
from shapely.geometry import Point, LineString, Polygon
from pyproj import Proj
# import hvplot.pandas 

from datetime import datetime, timedelta
# from holoviews import opts
import distinctipy
import haversine

import types

import warnings
warnings.filterwarnings('ignore')

import BandwidthConstraintCompressor as BWCC


import warnings
warnings.filterwarnings('ignore') # ignore timezone removal during data cleaning
# mpd.show_versions()




# Type conversions
def convert_points_trips(points):
    """
    Agregate a dataframe with TGeomInst to TGeomSeq
    """
    trajectories = points.groupby('trip_id').aggregate(
        {
            'point': lambda x: TGeomPointSeq.from_instants(x, upper_inc=True)
        }
    ).rename({'point': 'trajectory'}, axis=1)
    return trajectories


def convert_trajectory_points(trip_id, trajectory, sort=True):
    """Convert a single trajectory into a dataframe of points."""
    instants = trajectory.instants()

    df = pd.DataFrame.from_records(((trip_id, inst) for inst in instants), columns=["id", "point"])
    return df


def convert_trips_points(trips, sort=True):
    """
    Dissagregate a dataframe with TGeomSeq to TGeomInst
    """
    point_generator = ((trip_id, instant) for trip_id, row in trips.iterrows() for instant in row.trajectory.instants())

    points = pd.DataFrame.from_records(point_generator, columns = ["id", "point"])
    if sort:
        # print(points.head())
        points = points.sort_values(by="point") # , key=lambda x: x.str.lower())
    return points



# Plotting

def plot_trips(sequences, colors=None, labels=[]):
    """

    """
    fig, axes = plt.subplots()

    sequences = [sequences] if type(sequences) != list else sequences
        

    for i, s in enumerate(sequences):
        if type(colors) == list:
            s.plot(color=colors[i])
        elif type(colors) ==  types.GeneratorType:
            s.plot(color=next(colors))
        else:
            s.plot()

    # lab = labels[i] if i < len(labels) else None
    if len(labels) > 0:
        if len(colors) > 0:
            repetitions = len(colors)//len(labels)
            legend_elements = [Line2D([0], [0], color=colors[i*repetitions], lw=4, label=labels[i]) for i in range(len(labels))]

        plt.gca().legend(handles=legend_elements, loc='best')
    
    plt.show()


def plot_trips_df(trips, algorithms):
    trajectories = []
    
    N = len(algorithms)
    colors = distinctipy.get_colors(N)
    
    color_gen = []
    
    for c, algo in zip(colors, algorithms):
        trajectories.extend(trips[algo])
        color_gen.extend([c for i in range(len(trips[algo]))])

    plot_trips(trajectories, colors=color_gen, labels=algorithms)



def extract_wkt_from_traj(traj):
    """Create a wkt representation from the movingPandas trip."""
    res = "["
    for row in traj.df.iloc():
        item = row.geometry.wkt + '@' + row.name.__str__()
        res += item + ','
    res = res[:-1] + ']'
    return res


def analyse_points(points):
    """Perfoms checks on the points:

        - no two point at the same time in the same trajectory.
    """
    mmsis = set(points.mmsi)

    for mmsi in mmsis:
        points_mmsi = points[points.mmsi == mmsi].point.tolist()

        print(mmsi, len(points_mmsi))
        error = 0
        for i, pt in enumerate(points_mmsi[:-1]):
            if points_mmsi[i+1].timestamp() == pt.timestamp():
                # print("error", pt, points_mmsi[i+1])
                error += 1
        if error > 0:
            print("errors:", error)
        



##########################################################################
######                Other Compression Techniques                 #######
##########################################################################
def compress_with_pymeos(trips, tolerence=0.001, synchronized=True):
    trips = trips.copy()
    trips['trajectory'] = trips['trajectory'].apply(lambda tr: tr.simplify_douglas_peucker(tolerence, synchronized=synchronized))
    return trips


def compress_trips_squish(trips, ratio=29, delta=timedelta(hours=24)):
    res = {}
    for mmsi, row in trips.iterrows():
        trajectory = row.trajectory
        nb_points = max(len(trajectory.instants())//ratio, 3)
        points = convert_trajectory_points(mmsi, trajectory)
        bwcc = BWCC.BandwidthConstraintCompressor(nb_points=nb_points, instants=points, trips=row)
        bwcc.window_size = delta
        bwcc.compress(strategy="SQUISH")
        res_mmsi = bwcc.trips
        
        for mmsi, row in res_mmsi.iterrows():
            res[mmsi] = row.trajectory
        
        #plot_results(trips.loc[mmsi], res)
    
    results = pd.DataFrame.from_records(((mmsi, trajectory) for mmsi, trajectory in res.items()), 
                                        columns=["id", "trajectory"], index="id")
    return results


def compress_trips_STTrace(trips, instants, npoints, delta=timedelta(hours=24)):
    bwcc = BWCC.BandwidthConstraintCompressor(nb_points=npoints, instants=instants, trips=trips, window_size=delta)
    bwcc.compress(strategy="STTrace")
    return bwcc.trips




def compress_mpd_trips(mpd_trips, tolerence):
    """Compress trips in the mpd format using mpd using top down time ratio algorithm."""
    generalizing_fct = lambda trip: mpd.TopDownTimeRatioGeneralizer(trip.trajectory).generalize(tolerance=tolerence)
    mpd_trips['trajectory'] = mpd_trips.apply(generalizing_fct, axis=1)
    return mpd_trips

def convert_mpd_PyMeos(mpd_trips):
    trips =  mpd_trips.copy()
    trips['trajectory'] = trips.apply(lambda trip: TGeomPointSeq(string=extract_wkt_from_traj(trip.trajectory),
                                                                        normalize=False),
                                     axis=1)
    return trips 

def convert_PyMeos_mpd(trips):
    mpd_trips = trips.copy()
    mpd_trips['trajectory'] = mpd_trips.apply(lambda trip: mpd.Trajectory(trip.trajectory.to_dataframe(), 1) 
                                              , axis=1)
    
    return mpd_trips


def compress_trips_top_down_time_ratio(trips, tolerence=100):
    mpd_trips = convert_PyMeos_mpd(trips)
    mpd_compressed = compress_mpd_trips(mpd_trips, tolerence=tolerence)
    return convert_mpd_PyMeos(mpd_compressed)


##########################################################################
######                Assessing Compression Techniques             #######
##########################################################################

def assess_single_trajectory(compressed, original):
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
        
        distance = haversine.haversine((point.y, point.x), (point_compressed.y, point_compressed.x))*1000
        if distance > mx_distance:
            mx_distance = distance
        score += distance

        nmbr_instants += 1
        
        # print(nmbr_instants, distance, score)
        
    return score, nmbr_instants, mx_distance


# def assess_all_trajectories(compressed, originals):
#     trip_ids = originals.index.values.tolist()
#     if len(trip_ids) != len(set(trip_ids)):
#         print("problem with duplicates trip_ids in compresses")
#         
#     total_score = 0
#     total_points
#     for trip_id in trip_ids:
#         score, points = assess_single_trajectory(compressed=compressed.loc[trip_id].trajectory,
#                                                  original=originals.loc[trip_id].trajectory)
#         
#         total_score += score
#         total_points += points
#         
#     return total_score/total_points
        
def assess_algorithms(trips, algorithms, original_column):
    trip_ids = trips.index.values.tolist()
    
    scores = {algorithm : 0 for algorithm in algorithms}
    total_points = {algorithm : 0 for algorithm in algorithms}
    mx_distances = {algorithm : [] for algorithm in algorithms}
    
    for algorithm in algorithms:
        print(algorithm)
        for trip_id in trip_ids:
            score, points, dist = assess_single_trajectory(original=trips.loc[trip_id][original_column],
                                                           compressed=trips.loc[trip_id][algorithm])
            scores[algorithm] += score
            total_points[algorithm] += points
            mx_distances[algorithm].append(dist)

    
    for algorithm, s in scores.items():
        scores[algorithm] = s/total_points[algorithm]

    return scores, mx_distances


def compile_results(scores, distances):
    res = pd.DataFrame.from_records(scores)

    


def compile_trips(results, original_trips):
    """Results should be dico[name]:TGeomPointSequence"""

    all_compressed_trajectories = original_trips.rename({"trajectory": "Original"}, axis=1)
    
    for name, trajectory in results.items():
        # print(name, type(trajectory))
        all_compressed_trajectories = all_compressed_trajectories.join(trajectory, how="outer")
        all_compressed_trajectories = all_compressed_trajectories.rename({"trajectory": name}, axis=1)
        
    return all_compressed_trajectories
