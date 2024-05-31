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

# Add the project root to the PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import src.bwc.dr as BWC_DR
import src.bwc.sttrace as BWC_STTrace
import src.bwc.sttrace_delay as BWC_STTrace_Delay
import src.bwc.STTraceImp as BWC_STTrace_Imp
import src.bwc.STTraceImp_delay as BWC_STTrace_Imp_Delay
import src.bwc.squish as BWC_SQUISH
from src.helpers.data_loader import load_csv_to_df
from src.helpers.utility import convert_points_trips, assess_algorithms, compile_trips

import concurrent.futures

DIR = "data/preprocessed/"


def test_bwc():
    pymeos_initialize()
    tests = ["AIS_10"]  # , "birds_30"]

    # with concurrent.futures.ThreadPoolExecutor(max_workers=len(tests)) as executor:
    #     executor.map(evaluate_dataset_case, tests)
    for case in tests:
        evaluate_dataset_case(case)

    print("All tasks completed")


def evaluate_dataset_case(test_name):
    CONFIG_GLOBAL = ConfigObj("tests/bwc_tests_config.ini")

    # Same for all subtests
    CONFIG_TEST = CONFIG_GLOBAL[test_name]
    dataset = CONFIG_TEST["dataset"]
    columns = CONFIG_TEST["columns"]
    tests = CONFIG_TEST["subtests"]
    nys = Proj(CONFIG_TEST["proj"], preserve_units=True)
    bwc_sttrace_delta = timedelta(
        **{CONFIG_TEST["OPTREG_FREQ_UNIT"]: CONFIG_TEST.as_int("OPTREG_FREQ")}
    )

    points = load_csv_to_df(dataset, columns)
    trips = convert_points_trips(points)  # create trips here
    print(dataset, columns)

    algorithms = [
        "BWC_Squish",
        "BWC_STTrace",
        "BWC_STTrace_Delay",
        "BWC_STTrace_Imp_Delay",
        "BWC_STTrace_Imp",
        "BWC_DR",
    ]

    all_res = pd.DataFrame(index=algorithms)
    all_mean_delays = pd.DataFrame(index=algorithms)
    for test in tests:
        print(test)
        CONFIG = CONFIG_GLOBAL[test]  # subtest config

        npoints = CONFIG.as_int("NPOINTS")
        window_unit = CONFIG["WINDOW_SIZE_UNIT"]
        delta = {window_unit: CONFIG.as_int("WINDOW_SIZE")}
        # CONFIG["WINDOW_LENGTH"] = timedelta(**delta)
        window_length = timedelta(**delta)

        eval_delta = bwc_sttrace_delta / 2

        res, delays = compress_and_evaluate(
            points,
            trips,
            algorithms,
            w_length=window_length,
            eval_delta=eval_delta,
            nys=nys,
            npoints=npoints,
            bwc_sttrace_delta=bwc_sttrace_delta,
        )

        res = res.rename(columns={"avg_error": test})
        delays = delays.rename(columns={"avg. delay": test})
        all_res = pd.merge(all_res, res[test], left_index=True, right_index=True)
        all_mean_delays = pd.merge(all_mean_delays, delays[test], left_index=True, right_index=True)


    print("distances:")
    print(all_res)
    print()
    print("delays:")
    print(all_mean_delays)
    all_res.to_csv("res/bwc_compression/all.csv", mode="w")


def compress_and_evaluate(points, trips, algorithms, **kwargs):
    compressed_trajectories, delays = {}, {}
    compressed_trajectories, delays = compress_bwc(points, trips, algorithms, compressed_trajectories, delays, **kwargs)

    all_compressed_trajectories = compile_trips(compressed_trajectories, trips)
    

    res = assess_compression(all_compressed_trajectories, algorithms, kwargs["eval_delta"])
    mean_delays = analyse_delays(delays)

    return res, mean_delays


def compress_bwc(points, trips, algos, compressed_trajectories={}, delays={}, **kwargs):
    if "BWC_STTrace_Delay" in algos:
        print("bwcsttrace_delay start")
        bwc_sttrace_delay = BWC_STTrace_Delay.BWC_STTrace_Delay(
            points,
            window_lenght=kwargs["w_length"],
            limit=kwargs["npoints"],
            nys=kwargs["nys"],
        )
        bwc_sttrace_delay.compress()
        compressed_trajectories["BWC_STTrace_Delay"] = bwc_sttrace_delay.trips
        delays["BWC_STTrace_Delay"] = bwc_sttrace_delay.delays
        print("BWCCSTTRace_Delay finished")
    if "BWC_STTrace_Imp_Delay" in algos:
        print("bwcsttraceImp_delay start")
        bwc_sttrace_imp_delay = BWC_STTrace_Imp_Delay.BWC_STTrace_Imp_Delay(
            points,
            window_lenght=kwargs["w_length"],
            limit=kwargs["npoints"],
            nys=kwargs["nys"],
            init_trips=trips,
            eval_delta=kwargs["bwc_sttrace_delta"],
        )
        bwc_sttrace_imp_delay.compress()
        compressed_trajectories["BWC_STTrace_Imp_Delay"] = bwc_sttrace_imp_delay.trips
        delays["BWC_STTrace_Imp_Delay"] = bwc_sttrace_imp_delay.delays
        print("BWCCSTTRaceImp_Delay finished")
    if "BWC_STTrace" in algos:
        print("bwcsttrace start")
        bwc_sttrace = BWC_STTrace.BWC_STTrace(
            points,
            window_lenght=kwargs["w_length"],
            limit=kwargs["npoints"],
            nys=kwargs["nys"],
        )
        bwc_sttrace.compress()
        compressed_trajectories["BWC_STTrace"] = bwc_sttrace.trips
        delays["BWC_STTrace"] = bwc_sttrace.delays
        print("BWCCSTTRace finished")

    if "BWC_Squish" in algos:
        print("bwcsquish start")
        bwc_squish = BWC_SQUISH.BWC_SQUISH(
            points,
            window_lenght=kwargs["w_length"],
            limit=kwargs["npoints"],
            nys=kwargs["nys"],
        )
        bwc_squish.compress()
        compressed_trajectories["BWC_Squish"] = bwc_squish.trips
        delays["BWC_Squish"] = bwc_squish.delays
        print("bwcsquish finished")


    if "BWC_STTrace_Imp" in algos:
        print("bwcsttraceImp start")
        bwc_sttrace_imp = BWC_STTrace_Imp.BWC_STTrace_Imp(
            points,
            window_lenght=kwargs["w_length"],
            limit=kwargs["npoints"],
            nys=kwargs["nys"],
            init_trips=trips,
            eval_delta=kwargs["bwc_sttrace_delta"],
        )
        bwc_sttrace_imp.compress()
        compressed_trajectories["BWC_STTrace_Imp"] = bwc_sttrace_imp.trips
        delays["BWC_STTrace_Imp"] = bwc_sttrace_imp.delays
        print("BWCCSTTRaceImp finished")

    if "BWC_DR" in algos:
        print("bwc DR start")
        bwc_dr = BWC_DR.BWC_DR(
            points,
            window_lenght=kwargs["w_length"],
            limit=kwargs["npoints"],
            nys=kwargs["nys"],
        )
        bwc_dr.compress()
        bwc_dr.finalize_trips()
        compressed_trajectories["BWC_DR"] = bwc_dr.finalized_trips
        delays["BWC_DR"] = bwc_dr.delays
        print("BWC_DR finished")



    return compressed_trajectories, delays


def assess_compression(all_compressed_trajectories, algorithms, eval_delta):
    num_points = {}
    for algo in algorithms:
        # print(algo)
        if algo in all_compressed_trajectories:
            num_point = sum(
                [
                    len(trajectory.instants())
                    for trajectory in all_compressed_trajectories[algo]
                ]
            )
        else:
            num_point = 0
        num_points[algo] = num_point

    scores, distances = assess_algorithms(
        all_compressed_trajectories,
        algorithms,
        "Original",
        precision=eval_delta,
    )

    res = compile_results(scores, distances, num_points, algos=algorithms)
    # res.to_csv("res/" + CONFIG["testname"] + ".csv", mode="a")
    print()
    print(res)
    return res

def analyse_delays(delays):
    mean_delays = {key: np.mean(v) for key, v in delays.items()}
    mean_delays = pd.DataFrame.from_dict(mean_delays, orient="index", columns=["avg. delay"])
    print(mean_delays)
    print()
    return mean_delays



def compile_results(scores, distances, num_points, algos):
    columns = ["#remaining", "avg_error", "avg_max", "max_max", "med_max"]
    res = defaultdict(list)
    for algo in algos:
        res[algo].append(num_points[algo])
        res[algo].append(scores[algo])

    for algo, algo_distances in distances.items():
        res[algo].append(sum(algo_distances) / len(algo_distances))
        res[algo].append(max(algo_distances))
        res[algo].append(np.median(algo_distances))

    res = dict(res)
    res = pd.DataFrame.from_dict(res, orient="index", columns=columns)
    return res


if __name__ == "__main__":
    test_bwc()
