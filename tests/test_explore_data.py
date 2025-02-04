
from pyproj import Proj

import sys
import os


import matplotlib.pyplot as plt
from pymeos import pymeos_initialize
from pymeos.plotters import TemporalPointSequencePlotter, TemporalSequencePlotter
from tqdm.notebook import tqdm
import contextily as cx

# Add the project root to the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.helpers.data_handler import load_csv_to_df
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
    # for _, vessel in trips.iterrows():
    #     vessel["trajectory"].plot(axes=ax)
    TemporalPointSequencePlotter.plot_sequences_xy(trips['trajectory'], axes=ax, show_markers=False, show_grid=False)
    cx.add_basemap(ax, crs=4326, source=cx.providers.CartoDB.Voyager)
    plt.show()
    return


if __name__ == "__main__":
    pymeos_initialize()
    dataset = "ais_20210101"
    dataset = "flights"
    trips, points = check_squish(dataset)
    trips, points = load_data(dataset)
    # trips, points = load_data(dataset)
    # ts = [x.timestamp() for x in points.point]

    # print(min(ts), max(ts))
    # plot_data(trips)
