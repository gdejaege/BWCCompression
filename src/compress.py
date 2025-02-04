"""
Script Name: compress.py

Description: Compress the different cases with all compression algorithms

Naming Conventions:
- dataset: the original data to be compressed (ex: AIS, Birds, ...).
- case: a dataset and a compression ratio (ex: AIS_10, for 10 percent of AIS).
- window: for algorithms working with time windows, a duration of that window.
          The windows are described as "subconfigurations" of a case.
"""
import pprint

import inspect
from pathlib import Path

import numpy as np
import pandas as pd
from configobj import ConfigObj

from pymeos import pymeos_initialize

from datetime import timedelta
from pyproj import Proj
import sys
import os

import src.bwc.random as BWC_Random
from bwc.uniform import BWC_uniform

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import src.bwc.dr as BWC_DR
import src.bwc.sttrace as BWC_STTrace
import src.bwc.sttrace_delay as BWC_STTrace_Delay
import src.bwc.STTraceImp as BWC_STTrace_Imp
import src.bwc.STTraceImp_delay as BWC_STTrace_Imp_Delay
import src.bwc.squish as BWC_SQUISH
import src.bwc.squish_delay as BWC_SQUISH_Delay
from src.helpers.data_handler import filename, load_csv_to_df, save_df_to_csv, save_compressed_trajectories, load_config
from src.helpers.utility import convert_points_trips, convert_trips_points


DIR = "data/preprocessed/"
OUT = "data/compressed/"
OUT_DELAYS = "res/delays"
CONFIG_PATH = "tests/config_compress.ini"


def compress(algorithms, datasets, compression_ratios):
    print(datasets, algorithms, compression_ratios)
    for dataset in datasets:
        for compression_ratio in compression_ratios:
            compress_case(dataset, compression_ratio, algorithms)
    return

def compress_case(dataset, compression_ratio, algorithms):
    """Compress and save the compressed trajectories (as well as the delays).

    The delays are computed during the compression since they cannot be inferred
    later from the compressed trajectories. They must thus be saved here as well.
    """
    cfg = load_config(dataset, compression_ratio)
    windows = cfg["windows"]
    limits = cfg["points"]
    # pprint.pprint(cfg)
    points = load_csv_to_df(dataset, cfg["columns"], quality="preprocessed")
    trips = convert_points_trips(points)  # create trips here
    cfg["trips"] = trips

    all_mean_delays = pd.DataFrame(index=[a.__class__.__name__ for a in algorithms])
    all_mn_delays={}

    # if cfg["algorithms"]:
    #     delays = compress(
    #         points,
    #         trips,
    #         cfg["algorithms"],
    #         cfg,
    #     )
    #     delays = delays.rename(columns={"avg. delay": "No-window"})
    #     all_mean_delays = pd.merge(
    #         all_mean_delays, delays["No-window"], left_index=True, right_index=True
    #     )

    for window, limit in zip(windows, limits):
        cfg["window_length"] = window
        cfg["limit"] = limit
        window_name = dataset + "_" + str(window)
        # print("Compressing", dataset, window)

        delays = compress_algorithms(points=points, algorithms=algorithms, cfg=cfg)
        # print("finishing", window, limit, delays)
        # delays = delays.rename(columns={"avg. delay": window_name})
        all_mn_delays[window_name] = delays
        # print("indices:")
        # print(all_mean_delays.index)
        # print(delays.index)
        # print("heads:")
        # print("delays", delays.head())
        # print("all:", all_mean_delays.head())

    print()
    print("delays:")
    all_mn_delays = pd.DataFrame.from_dict(all_mn_delays)
    print(all_mn_delays)
    folder = "res/delays/" + cfg["case"] + "/"
    Path(folder).mkdir(parents=True, exist_ok=True)
    all_mn_delays.to_csv(folder+"all.csv", mode="a")
    # all_res.to_csv("res/bwc_compression/all.csv", mode="a")

def compress_algorithms(points, algorithms, cfg):
    def filter_args(cls, param_dict):
        sig = inspect.signature(cls.__init__)
        # Extract argument names (excluding 'self')
        arg_names = [param for param in sig.parameters if param != 'self' and param != "points"]

        # Filter the params dictionary to match the class constructor
        filtered_args = {k: v for k, v in param_dict.items() if k in arg_names}
        return filtered_args

    delays = {}
    for algo in algorithms:
        print(algo, "start", end=": ")
        params = filter_args(algo, cfg)
        compressor = algo(points,  **params)
        compressor.compress()

        compressed_points = convert_trips_points(compressor.finalized_trips)
        # compressed_rebuild_trips = convert_points_trips(compressed_points)
        save_compressed_trajectories(algo, compressed_points, cfg)
        delays[algo] = compressor.delays
        # print(compressor.delays)
        # print("end:", len(compressor.finalized_trips), len(compressed_rebuild_trips), len(compressed_points))
        # print()

    return analyse_delays(delays)

def analyse_delays(delays):
    mean_delays = {key: np.mean(v) for key, v in delays.items()}
    return mean_delays


if __name__ == "__main__":
    pymeos_initialize()
    datasets = ["birds", "ais", "flights", "taxi"]
    datasets = ["taxi"]
    algorithms = [
        BWC_uniform,
        BWC_Random.BWC_Random,
        BWC_STTrace_Imp.BWC_STTrace_Imp,
        BWC_STTrace_Imp_Delay.BWC_STTrace_Imp_Delay,
        BWC_SQUISH.BWC_SQUISH,
        BWC_SQUISH_Delay.BWC_SQUISH_Delay,
        BWC_STTrace.BWC_STTrace,
        BWC_STTrace_Delay.BWC_STTrace_Delay,
        BWC_DR.BWC_DR,
    ]
    compression_ratios = [0.1, 0.3]
    compress(algorithms, datasets, compression_ratios)
