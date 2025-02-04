import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from configobj import ConfigObj

from pymeos import pymeos_initialize

from pyproj import Proj

import contextlib
import io

import warnings

warnings.filterwarnings( 
    "ignore")

# Function to mute the print statements temporarily
def mute_print():
    return contextlib.redirect_stdout(io.StringIO())


from src.helpers.data_handler import filename, load_csv_to_df, save_df_to_csv
from src.helpers.utility import convert_points_trips, convert_trips_points
from src.helpers.classical_compression import (
    DeadReckoning,
    classical_squish,
    classical_STTrace,
    compress_trips_top_down_time_ratio,
)

CONFIG_PATH = "tests/config_compress_classical.ini"


def test_classical():
    pymeos_initialize()
    cases = ["AIS_10", "birds_10", "flights_10", "AIS_30", "birds_30", "flights_30"]

    cases = ConfigObj(CONFIG_PATH)["GLOBAL"]["CASES"]  # type: ignore

    # with concurrent.futures.ThreadPoolExecutor(max_workers=len(tests)) as executor:
    #     executor.map(evaluate_dataset_case, tests)
    for case in cases:
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
    cfg["NPOINTS"] = CONFIG.as_int("NPOINTS")  # type: ignore
    cfg["TDTR_TOLERENCE"] = CONFIG.as_float("TDTR_TOLERENCE")  # type: ignore
    cfg["DR_THRESH"] = CONFIG.as_float("DR_THRESH")  # type: ignore
    cfg["RATIO"] = CONFIG.as_float("RATIO")  # type: ignore
    cfg["algorithms"] = ConfigObj(CONFIG_PATH)["GLOBAL"]["ALGORITHMS"]  # type: ignore

    return cfg


def compress_case(case):
    """Compress and save the compressed trajectories (as well as the delays).

    The delays are computed during the compression and can not be infered from the
    compressed trajectories. They must thus be saved here as well.
    """
    cfg = load_test_config(case)
    points = load_csv_to_df(cfg["dataset"], cfg["columns"], quality="preprocessed")
    # trips = convert_points_trips(points)  # create trips here
    print("Compressing ", case, cfg["columns"])
    compress(points, cfg)
    print()


def compress(points, cfg):
    def check_to_compress(algo, cfg):
        fn = filename(
            dataset=cfg["dataset"],
            quality="compressed",
            case=cfg["case"],
            algorithm=algo,
            # window=cfg.get("window_name", ""),
        )
        # print(fn, (not os.path.exists(fn)))
        return (not os.path.exists(fn)) or ConfigObj(CONFIG_PATH)["GLOBAL"].as_bool("RECOMPUTE")  # type: ignore

    for algo in cfg["algorithms"]:
        if not check_to_compress(algo, cfg):
            print(algo, cfg.get("window_name"), "already compressed")
            continue
        print(algo, "start", end=" ")
        # DR_RT to move to another file !
        if algo == "SQUISH":
            with mute_print():
                compressed_trips = classical_squish(
                    points,
                    ratio=cfg["RATIO"],
                    proj=cfg["proj"],
                )
        elif algo == "DR":
            compressed_trips = DeadReckoning(
                instants=points,
                threshold=cfg["DR_THRESH"],
                proj=cfg["proj"],
            )
        elif algo == "STTrace":
            compressed_trips = classical_STTrace(
                points,
                npoints=cfg["NPOINTS"],
                proj=cfg["proj"],
            )
        else:  # algo == "TDTR":
            compressed_trips = compress_trips_top_down_time_ratio(
                points,
                tolerence=cfg["TDTR_TOLERENCE"],
            )

        # compressed_trajectories[algo] = compressor.trips
        compressed_points = convert_trips_points(compressed_trips)
        # print(compressed_points.head())
        save_compressed_trajectories(algo, compressed_points, cfg)
        print(algo, "end:", len(compressed_trips), len(compressed_points))
        print("aim:", len(compressed_points) - cfg["NPOINTS"])
        print()

    return


def save_compressed_trajectories(algo, compressed_points, cfg):
    # print(cfg["case"], algo, cfg.get("window_name", ""))
    save_df_to_csv(
        dataset_name=cfg["dataset"],
        df=compressed_points,
        quality="compressed",
        case=cfg["case"],
        algorithm=algo,
        # window=cfg.get("window_name", ""),
    )
    return


if __name__ == "__main__":
    test_classical()
