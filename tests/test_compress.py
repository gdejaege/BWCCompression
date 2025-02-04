"""
Script Name: test_compression.py

Description: Compress the different cases with all compression algorithms

Naming Conventions:
- dataset: the original data to be compressed (ex: AIS, Birds, ...).
- case: a dataset and a compression ratio (ex: AIS_10, for 10 percent of AIS).
- window: for algorithms working with time windows, a duration of that window. 
          The windows are described as "subconfigurations" of a case.
"""

import numpy as np
import pandas as pd
from configobj import ConfigObj

# from numpy.lib import test
from pymeos import pymeos_initialize


from datetime import timedelta

from pyproj import Proj

import sys
import os

# Add the project root to the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import src.bwc.dr as BWC_DR
import src.bwc.dr_rt as BWC_DR_RT
import src.bwc.sttrace as BWC_STTrace
import src.bwc.sttrace_delay as BWC_STTrace_Delay
import src.bwc.STTraceImp as BWC_STTrace_Imp
import src.bwc.STTraceImp_delay as BWC_STTrace_Imp_Delay
import src.bwc.squish as BWC_SQUISH
import src.bwc.squish_delay as BWC_SQUISH_Delay
from src.helpers.data_handler import filename, load_csv_to_df, save_df_to_csv
from src.helpers.utility import convert_points_trips, convert_trips_points

# import concurrent.futures

DIR = "data/preprocessed/"
OUT = "data/compressed/"
OUT_DELAYS = "res/delays"
CONFIG_PATH = "tests/config_compress.ini"


def test_bwc():
    pymeos_initialize()
    tests = ["birds_30"]
    tests = ["flights_10"]  # , "birds_30"]
    tests = ["AIS_10"]  # , "birds_30"]
    tests = ConfigObj(CONFIG_PATH)["GLOBAL"]["TESTS"]  # type: ignore

    # with concurrent.futures.ThreadPoolExecutor(max_workers=len(tests)) as executor:
    #     executor.map(evaluate_dataset_case, tests)
    for case in tests:
        compress_case(case)

    print("All cases compressed")
    return


def load_test_config(case):
    """Return configuration for the different case and its windows.

    One dict for the algorithms without time windowing + one dict per window of the case.
    """
    CONFIG = ConfigObj(CONFIG_PATH)[case]
    cfg = {}

    cfg["case"] = case
    cfg["dataset"] = CONFIG["DATASET"]  # type: ignore
    cfg["columns"] = CONFIG["COLUMNS"]  # type: ignore
    cfg["proj"] = Proj(CONFIG["PROJ"], preserve_units=True)  # type: ignore
    # cfg["algorithms"] = ["BWC_DR_RT"]
    # maybe put in config file since its also used by evaluation
    cfg["algorithms"] = ConfigObj(CONFIG_PATH)["GLOBAL"]["ALGORITHMS"]  # type: ignore
    print(cfg["algorithms"])

    cfg["algorithms_windowed"] = ConfigObj(CONFIG_PATH)["GLOBAL"]["ALGORITHMS_WINDOWED"]  # type: ignore

    cfg["bwc_sttrace_delta"] = timedelta(
        **{CONFIG["OPTREG_FREQ_UNIT"]: CONFIG.as_int("OPTREG_FREQ")}  # type: ignore
    )

    cfg["process_time"] = timedelta(**{CONFIG["PROCESS_TIME_UNIT"]: CONFIG.as_float("PROCESS_TIME")})  # type: ignore

    # cfg["windows"] = CONFIG["WINDOWS"]

    cfg_windows = {}
    if cfg["algorithms_windowed"]:
        for window_name in CONFIG["WINDOWS"]:  # type: ignore
            # print("create config for", window_name)
            CONFIG = ConfigObj(CONFIG_PATH)[window_name]
            cfg_w = {}

            cfg_w["window_name"] = window_name
            cfg_w["npoints"] = CONFIG.as_int("NPOINTS")  # type: ignore
            cfg_w["window_unit"] = CONFIG["WINDOW_SIZE_UNIT"]  # type: ignore
            cfg_w["delta"] = {cfg_w["window_unit"]: CONFIG.as_int("WINDOW_SIZE")}  # type: ignore
            # cfg_w["# CONFIG["WINDOW_LENGTH"] = timedelta(**delta)
            cfg_w["window_length"] = timedelta(**cfg_w["delta"])

            cfg_w["eval_delta"] = cfg["bwc_sttrace_delta"] / 2

            cfg_w.update(cfg)
            cfg_windows[window_name] = cfg_w

    # print()
    return cfg, cfg_windows


def compress_case(case):
    """Compress and save the compressed trajectories (as well as the delays).

    The delays are computed during the compression and can not be infered from the
    compressed trajectories. They must thus be saved here as well.
    """
    cfg, cfg_windows = load_test_config(case)
    points = load_csv_to_df(cfg["dataset"], cfg["columns"], quality="preprocessed")
    trips = convert_points_trips(points)  # create trips here
    # print("Compressing ", cfg["dataset"], cfg["columns"])
    # print()

    all_mean_delays = pd.DataFrame(index=cfg["algorithms"] + cfg["algorithms_windowed"])

    """
    iterations = (((cfg["algorithms"], cfg, "No-window")),  
                   *((cfg_window["algorithms_windowed"], cfg_window, window_name)    
                      for window_name, cfg_window in cfg_windows.items())
                 )

    for iteration in iterations:
        delays = compress(points, trips, iteration[0], iteration[1],)
        delays = delays.rename(columns={"avg. delay": iteration[2]})
        all_mean_delays = pd.merge(
            all_mean_delays, delays[iteration[2]], left_index=True, right_index=True
        )
    """
    if cfg["algorithms"]:
        delays = compress(
            points,
            trips,
            cfg["algorithms"],
            cfg,
        )
        delays = delays.rename(columns={"avg. delay": "No-window"})
        all_mean_delays = pd.merge(
            all_mean_delays, delays["No-window"], left_index=True, right_index=True
        )

    # all_res = pd.DataFrame(index=cfg["algorithms"])
    for window_name, cfg_window in cfg_windows.items():
        print("Compressing", window_name)

        # cfg_window.update(cfg)  # done in the config creation

        delays = compress(points, trips, cfg_window["algorithms_windowed"], cfg_window)

        delays = delays.rename(columns={"avg. delay": window_name})
        all_mean_delays = pd.merge(
            all_mean_delays, delays[window_name], left_index=True, right_index=True
        )
        print()

    # print("distances:")
    # print(all_res)
    print()
    print("delays:")
    print(all_mean_delays)
    all_mean_delays.to_csv("res/delays/" + cfg["case"] + "/all.csv", mode="a")
    # all_res.to_csv("res/bwc_compression/all.csv", mode="a")


def compress(points, trips, algorithms, cfg, delays={}):
    def check_to_compress(algo, cfg):
        fn = filename(
            dataset=cfg["dataset"],
            quality="compressed",
            case=cfg["case"],
            algorithm=algo,
            window=cfg.get("window_name", ""),
        )
        # print(fn, (not os.path.exists(fn)))
        return (not os.path.exists(fn)) or ConfigObj(CONFIG_PATH)["GLOBAL"].as_bool("RECOMPUTE")  # type: ignore

    for algo in algorithms:
        if not check_to_compress(algo, cfg):
            print(algo, cfg.get("window_name"), "already compressed")
            continue
        print(algo, "start", end=": ")
        # DR_RT to move to another file !
        if algo == "BWC_DR_RT":
            compressor = BWC_DR_RT.BWC_DR_RT(
                points,
                process_time=cfg["process_time"],
                proj=cfg["proj"],
            )
        elif algo == "BWC_DR":
            compressor = BWC_DR.BWC_DR(
                points,
                window_lenght=cfg["window_length"],
                limit=cfg["npoints"],
                proj=cfg["proj"],
            )
        elif algo == "BWC_Squish":
            compressor = BWC_SQUISH.BWC_SQUISH(
                points,
                window_lenght=cfg["window_length"],
                limit=cfg["npoints"],
                proj=cfg["proj"],
            )
        elif algo == "BWC_Squish_Delay":
            compressor = BWC_SQUISH_Delay.BWC_SQUISH_Delay(
                points,
                window_lenght=cfg["window_length"],
                limit=cfg["npoints"],
                proj=cfg["proj"],
            )
        elif algo == "BWC_STTrace":
            compressor = BWC_STTrace.BWC_STTrace(
                points,
                window_lenght=cfg["window_length"],
                limit=cfg["npoints"],
                proj=cfg["proj"],
            )

        elif algo == "BWC_STTrace_Delay":
            compressor = BWC_STTrace_Delay.BWC_STTrace_Delay(
                points,
                window_lenght=cfg["window_length"],
                limit=cfg["npoints"],
                proj=cfg["proj"],
            )
        elif algo == "BWC_STTrace_Imp_Delay":
            compressor = BWC_STTrace_Imp_Delay.BWC_STTrace_Imp_Delay(
                points,
                window_lenght=cfg["window_length"],
                limit=cfg["npoints"],
                proj=cfg["proj"],
                init_trips=trips,
                eval_delta=cfg["bwc_sttrace_delta"],
            )
        else:
            compressor = BWC_STTrace_Imp.BWC_STTrace_Imp(
                points,
                window_lenght=cfg["window_length"],
                limit=cfg["npoints"],
                proj=cfg["proj"],
                init_trips=trips,
                eval_delta=cfg["bwc_sttrace_delta"],
            )

        compressor.compress()

        # print(compressor.finalized_trips.head())
        # compressed_trajectories[algo] = compressor.trips
        compressed_points = convert_trips_points(compressor.finalized_trips)
        compressed_rebuild_trips = convert_points_trips(compressed_points)
        # print(compressed_points.head())
        save_compressed_trajectories(algo, compressed_points, cfg)
        delays[algo] = compressor.delays
        print("end:", len(compressor.finalized_trips), len(compressed_rebuild_trips), len(compressed_points))
        # print()

    return analyse_delays(delays)


def save_compressed_trajectories(algo, compressed_points, cfg):
    # print(cfg["case"], algo, cfg.get("window_name", ""))
    save_df_to_csv(
        dataset_name=cfg["dataset"],
        df=compressed_points,
        quality="compressed",
        case=cfg["case"],
        algorithm=algo,
        window=cfg.get("window_name", ""),
    )
    return


def analyse_delays(delays):
    mean_delays = {key: np.mean(v) for key, v in delays.items()}
    mean_delays = pd.DataFrame.from_dict(
        mean_delays, orient="index", columns=["avg. delay"]
    )
    # print(mean_delays)
    # print()
    return mean_delays


if __name__ == "__main__":
    test_bwc()
