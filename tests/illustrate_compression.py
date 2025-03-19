import geopandas as gpd
from shapely.geometry import LineString, Point
from shapely.affinity import translate, scale
from datetime import timedelta

from shapely.affinity import rotate

import numpy as np
import pandas as pd
from pyproj import Proj

import sys
import os


import matplotlib.pyplot as plt
from pymeos import pymeos_initialize, TGeomPointSeq
from pymeos.plotters import TemporalPointSequencePlotter, TemporalSequencePlotter
from tqdm.notebook import tqdm
import contextily as cx

from bwc.dr import BWC_DR
from helpers.utility import convert_trip_points, convert_trips_points, filter_args
from plotters.plot_trajectories_tikz import project_points, get_scaling_factors, apply_scaling

# Add the project root to the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.helpers.data_handler import load_csv_to_df, load_config
from src.helpers.utility import (
    convert_points_trips,
)

import concurrent.futures

CONFIG_PATH = "tests/config_compress.ini"


def load_data(dataset_name, aglorithm=None):
    points = load_csv_to_df(
        dataset_name,
        ["id", "point"],
        quality="preprocessed",
    )
    trips = convert_points_trips(points)
    print(len(trips), len(points))
    return trips, points

def check_squish(dataset_name):
    points = load_csv_to_df(
        dataset_name,
        ["id", "point"],
        quality="compressed",
        algorithm="BWC_Squish",
        case="flights_10",
        window="flights_10_1m"
    )
    trips = convert_points_trips(points)
    print(len(trips), len(points))
    return trips, points

def plot_data(trips):
    fig, ax = plt.subplots(figsize=(12, 6))
    TemporalPointSequencePlotter.plot_sequences_xy(trips['trajectory'], axes=ax, show_markers=False, show_grid=False)
    cx.add_basemap(ax, crs=4326, source=cx.providers.CartoDB.Voyager)
    plt.show()
    return

def compress_single_trip(trip):
    points =  convert_trips_points(trip)
    proj = Proj("EPSG:32632", preserve_units=True)
    window_length = timedelta(minutes=15)
    limit = 30
    compressor = BWC_DR(points, window_length, limit, proj)
    compressor.compress()
    compressed_trips = compressor.finalized_trips
    return compressed_trips


def plot_single_trip_compressed_points(trips):
    def to_tikz(points):
        return " -- ".join(f"({p.x},{p.y})" for p in points)
    original_trajectory = [instant.value() for instant in trips.iloc[0]["trajectory"].instants()]
    compressed_trajectory = [instant.value() for instant in trips.iloc[1]["trajectory"].instants()]
    # Rotate the geometries (angle in degrees, counterclockwise)
    angle = 60  # Change this value to rotate more/less
    origin = (4.35, 50.85)  # Rotation origin (center of rotation)

    original_trajectory = [rotate(pt, angle, origin=origin) for pt in original_trajectory]
    compressed_trajectory = [rotate(pt, angle, origin=origin) for pt in compressed_trajectory]

    gdf1 = gpd.GeoDataFrame(geometry=original_trajectory, crs="EPSG:4326")
    gdf2 = gpd.GeoDataFrame(geometry=compressed_trajectory, crs="EPSG:4326")

    # Reproject to UTM Zone 32N (Denmark's projection)
    gdf1 = gdf1.to_crs("EPSG:25832")
    gdf2 = gdf2.to_crs("EPSG:25832")

    # Get min values for shifting to (0,0)
    min_x = min(gdf1.geometry.x.min(), gdf2.geometry.x.min())
    min_y = min(gdf1.geometry.y.min(), gdf2.geometry.y.min())

    # Shift all points so the smallest point is at (0,0)
    shifted_gdf1 = gdf1.geometry.apply(lambda p: translate(p, xoff=-min_x, yoff=-min_y))
    shifted_gdf2 = gdf2.geometry.apply(lambda p: translate(p, xoff=-min_x, yoff=-min_y))

    # Scale down to a reasonable range (e.g., max 10)
    max_x = max(shifted_gdf1.x.max(), shifted_gdf2.x.max())
    max_y = max(shifted_gdf1.y.max(), shifted_gdf2.y.max())
    scale_factor = 10 / max(max_x, max_y)  # Normalize to max ~10

    gdf1 = shifted_gdf1.apply(lambda p: scale(p, xfact=scale_factor, yfact=scale_factor, origin=(0, 0)))
    gdf2 = shifted_gdf2.apply(lambda p: scale(p, xfact=scale_factor, yfact=scale_factor, origin=(0, 0)))

    # Shift the second list downward (in meters)
    shift_value = 2  # Adjust as needed
    lowered_list2 = [translate(p, yoff=-shift_value) for p in gdf2.geometry]

    # Translate the second list downward for separation in TikZ
    shift_value = 0.01  # Adjust as needed
    #lowered_list2 = [translate(p, yoff=-shift_value) for p in rotated_list2]

    # Generate TikZ code
    tikz_code = f"""
    \\begin{{tikzpicture}}

    % First line (upper)
    \\draw{to_tikz(gdf1.geometry)};

    % Second line (shifted downward)
    \\draw{to_tikz(lowered_list2)};

    \\end{{tikzpicture}}
    """

    # Print or save the TikZ code
    print(tikz_code)
    print(original_trajectory)
    # Convert lists to GeoDataFrames
    gdf_line = gpd.GeoDataFrame(geometry=[LineString(original_trajectory)], crs="EPSG:4326")
    gdf_points = gpd.GeoDataFrame(geometry=compressed_trajectory, crs="EPSG:4326")

    # Plot
    fig, ax = plt.subplots(figsize=(6, 6))
    gdf_line.plot(ax=ax, color="blue", linewidth=2, label="Connected Line")
    gdf_points.plot(ax=ax, color="red", markersize=50, label="Red Points", alpha=0.8)

    # Remove axes for a clean look
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_frame_on(False)
    plt.show()


def load_trajectory(dataset, id, projection, case_algorithm_windows, pt_range):
    init_points_all = load_csv_to_df(dataset, ["id", "point"], quality="preprocessed")
    init_points_traj = init_points_all[init_points_all["id"] == id]["point"].tolist()[pt_range[0]:pt_range[1]]
    print(len(init_points_traj))
    last_point = init_points_traj[-1]
    start = init_points_traj[0].timestamp()
    end = init_points_traj[-1].timestamp()
    print(start, end)

    init_points_traj.sort(key=lambda x: x.timestamp())
    init_points_traj = [pt.value() for pt in init_points_traj]
    init_points_traj = project_points(init_points_traj, projection)
    scaling_factors = get_scaling_factors(init_points_traj)
    init_points_traj = apply_scaling(init_points_traj, scaling_factors)

    compressions = {}

    compression_ratio = 0.3
    cfg = load_config(dataset, compression_ratio)
    points = load_csv_to_df(dataset, cfg["columns"], quality="preprocessed")
    points = points[points["id"] == id]
    print(points.head())
    trips = convert_points_trips(points)
    cfg["trips"] = trips
    cfg["bwc_sttrace_delta"] = timedelta(seconds=30)
    cfg["window_length"] = timedelta(minutes=7)
    cfg["limit"] = 18

    for name, case_algorithm_window in case_algorithm_windows.items():
        case, algo_name, window, algo = case_algorithm_window
        # print(name, case, algo, window)
        old = False
        if old:
            compressed_points = load_csv_to_df(dataset, ["id", "point"], quality="compressed", case=case,
                                               algorithm=algo, window=window)

            compressed_points = compressed_points[compressed_points["id"] == id]["point"].tolist()
        else:
            params = filter_args(algo, cfg)
            compressor = algo(points,  **params)
            compressor.compress()

            compressed_points = convert_trips_points(compressor.finalized_trips)
            # print(compressed_points.head())
            compressed_points = compressed_points["point"].tolist()



        compressed_points = [pt.value() for pt in compressed_points if pt.timestamp() >= start and pt.timestamp() <= end]
        compressed_points.append(last_point.value())
        compressed_points = project_points(compressed_points, projection)
        compressed_points = apply_scaling(compressed_points, scaling_factors)
        print(name, len(compressed_points))
        compressions[name] = compressed_points

    return init_points_traj, compressions



def single_trip(trip, mx_instants=1000):
    instants = trip["trajectory"].iloc[0].instants()[5100:5500]
    trajectory = TGeomPointSeq(instant_list=instants)
    trips = pd.DataFrame({"id": [0], "trajectory": [trajectory]})
    compressed_trips = compress_single_trip(trips)
    trips = pd.concat([trips, compressed_trips])
    for i, row in trips.iterrows():
        print(len(row.trajectory.instants()))

    print(trips.head())
    plot_single_trip_compressed_points(trips)
    return
    # fig, ax = plt.subplots(figsize=(12, 6))
    # TemporalPointSequencePlotter.plot_sequences_xy(trips['trajectory'], axes=ax, show_markers=False, show_grid=False)
    # cx.add_basemap(ax, crs=4326, source=cx.providers.CartoDB.Voyager)
    # plt.show()
    # return


if __name__ == "__main__":
    pymeos_initialize()
    dataset = "ais_20210101"
    trips, points = load_data(dataset)
    trips = trips.reset_index()
    trip = trips.loc[trips["id"] == 219026706]
    single_trip(trip)
    exit()
    # trips, points = load_data(dataset)
    # ts = [x.timestamp() for x in points.point]
    dfs = np.array_split(trips, 10)
    df = dfs[2]
    print(len(df))
    dfs = np.array_split(df, 10)
    print("dfs splitted")
    for i, dfi in enumerate(dfs[1:]):
        print(i)
        print(dfi.head())
        plot_data(dfi)
        input("input:")

    # print(min(ts), max(ts))
    print("finished")
