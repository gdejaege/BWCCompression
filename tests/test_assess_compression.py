from collections import defaultdict
import numpy as np
import pandas as pd
from configobj import ConfigObj

# from numpy.lib import test
from pymeos import pymeos_initialize


from datetime import timedelta

from pyproj import Proj

import sys
import os

from helpers.metrics import SED_trips

# Add the project root to the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.helpers.data_handler import load_csv_to_df
from src.helpers.utility import (
    # assess_single_trajectory,
    convert_points_trips,
    assess_algorithms,
    compile_trips,
)

import concurrent.futures

CONFIG_PATH = "tests/config_compress.ini"


def test_assess_compression():
    def load_uncompressed_dataset(case):
        CONFIG = ConfigObj(CONFIG_PATH)[case]  # type: ignore
        dataset_name = CONFIG["DATASET"]  # type: ignore
        return load_csv_to_df(
            dataset_name,
            ["id", "point"],
            quality="preprocessed",
        )

    pymeos_initialize()
    cases = ["birds_30"]
    cases = ["flights_10"]  # , "birds_30"]
    algorithms = ["SQUISH", "STTrace", "DR", "TDTR"]  # "BWC_DR_RT",]
    algorithms = []  # "BWC_DR_RT",]
    # algorithms = ["DR", "TDTR"]  # "BWC_DR_RT",]
    cases = ["AIS_10", "AIS_30", #]# , "flights_30", ]
             "birds_10", "birds_30", 
             "flights_10", "flights_30", 
             ]
    algorithms_windowed = [
        "BWC_Squish",
        "BWC_Squish_Delay",
        "BWC_STTrace",
        "BWC_STTrace_Delay",
        "BWC_STTrace_Imp",
        "BWC_STTrace_Imp_Delay",
        "BWC_DR",
    ]

    cases_original_trips = {}
    for case in cases:
        cases_original_trips[case] = convert_points_trips(
            load_uncompressed_dataset(case)
        )

    if algorithms:
        print("evaluation classical algorithms")
        evaluate_classical_algorithms(cases_original_trips, algorithms)

    # with concurrent.futures.ThreadPoolExecutor(max_workers=len(tests)) as executor:
    #     executor.map(evaluate_dataset_case, tests)

    if algorithms_windowed:
        print("evaluation windowed algorithms")
        for case, original_trips in cases_original_trips.items():
            evaluate_case_windowed(case, original_trips, algorithms_windowed)

    print("All cases evaluated completed")


def evaluate_classical_algorithms(cases_original_trips, algorithms):
    distances = {}  # pd.DataFrame(index=algorithms)
    for case, original_trips in cases_original_trips.items():
        dataset = load_dataset_name(case)
        precision = load_precision(case)
        for algo in algorithms:
            print(case, algo, end=":")
            points = load_csv_to_df(
                dataset,
                ["id", "point"],
                quality="compressed",
                case=case,
                algorithm=algo,
            )
            compressed_trips = convert_points_trips(points, remove_shorts=False)
            score = SED_trips(original_trips, compressed_trips, precision)

            distances[(algo, case)] = score
            print(score)

    res = pd.DataFrame.from_dict(distances, orient="index", columns=["metric"])
    res.index = pd.MultiIndex.from_tuples(res.index, names=["algorithm", "cases"])
    # print(res.head())
    res = res.reset_index()
    res = res.pivot(index="algorithm", columns="cases", values="metric")  # type: ignore

    res.to_csv("res/distances/classical.csv", mode="a")
    print(res)
    print()
    return res


def evaluate_case_windowed(case, original_trips, algorithms_windowed):
    def load_windows(case):
        CONFIG = ConfigObj(CONFIG_PATH)[case]  # type: ignore
        windows = CONFIG["WINDOWS"]  # type: ignore
        return windows

    windows = load_windows(case)
    dataset_name = load_dataset_name(case)
    precision = load_precision(case)

    distances = {}
    for algorithm in algorithms_windowed:
        print(case, algorithm)
        for window in windows:
            print(window, end=" ")
            compressed_trips = convert_points_trips(
                load_csv_to_df(
                    dataset_name,
                    ["id", "point"],
                    quality="compressed",
                    case=case,
                    algorithm=algorithm,
                    window=window,
                )
            )
            distances[(algorithm, window)] = SED_trips(
                original_trips, compressed_trips, precision
            )
            print()
    print("Compiling results")

    res = pd.DataFrame.from_dict(distances, orient="index", columns=["metric"])
    res.index = pd.MultiIndex.from_tuples(res.index, names=["algorithm", "window"])
    # print(res.head())
    res = res.reset_index()
    res = res.pivot(index="algorithm", columns="window", values="metric")  # type: ignore
    res.reindex(algorithms_windowed).reset_index()

    print(res)
    print()
    res.to_csv("res/distances/" + case + ".csv", mode="a")
    return res





def load_dataset_name(case):
    return ConfigObj(CONFIG_PATH)[case]["DATASET"]  # type: ignore


def load_precision(case):
    CONFIG = ConfigObj(CONFIG_PATH)[case]
    precision = (
        timedelta(
            **{CONFIG["OPTREG_FREQ_UNIT"]: CONFIG.as_int("OPTREG_FREQ")}  # type: ignore
        )
        / 2
    )
    return precision


if __name__ == "__main__":
    test_assess_compression()
