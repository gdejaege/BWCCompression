from collections import defaultdict
import numpy as np
from configobj import ConfigObj

from pymeos.db.psycopg2 import MobilityDB
from pymeos import *

from pyproj import Proj

import BWC_SQUISH
import BWC_STTrace
import BWC_STTrace_Imp

import DeadReckoning as DR
import BWC_DeadReckoning as BWC_DR

# import DBHandler
import DBHandler_csv as DBHandler
from utility import *


def compress_classic(points, trips, algos, results={}):
    nys = Proj(CONFIG_DATA["proj"], preserve_units=True)

    if "Classical_Squish" in algos:
        print("Classical_Squish start")
        squish = BWC_SQUISH.classical_squish(
            trips,
            ratio=CONFIG_CLASSIC.as_float("RATIO"),
            delta=timedelta(days=CONFIG_CLASSIC.as_int("CLASSIC_DELTA")),
            nys=nys,
        )
        results["Classical_Squish"] = squish
        print("squish finished")

    if "DR" in algos:
        print("DR start")
        dr = DR.DeadReckoning(
            instants=points, threshold=CONFIG_CLASSIC.as_int("DR_THRESH"), nys=nys
        )
        dr.compress()
        dr.finalize_trips()
        results["DR"] = dr.trips
        print("DR finished")

    if "TDTR" in algos:
        print("TDTR start")
        tdtr = compress_trips_top_down_time_ratio(
            trips, tolerence=CONFIG_CLASSIC.as_float("TDTR_TOLERENCE")
        )
        results["TDTR"] = tdtr
        print("top_down_ratio_finished")

    if "Classical_STTrace" in algos:
        print("Classical_STTrace start")
        sttrace = BWC_STTrace.classical_STTrace(
            trips=trips,
            instants=points,
            npoints=CONFIG_CLASSIC.as_int("NPOINTS_STTRACE"),
            delta=timedelta(days=CONFIG_CLASSIC.as_int("CLASSIC_DELTA")),
            nys=nys,
        )

        results["Classical_STTrace"] = sttrace
        print("sttrace finished")
    return results


def compress_bwc(points, trips, algos, results={}):
    nys = Proj(CONFIG_DATA["proj"], preserve_units=True)
    if "BWC_DR" in algos:
        print("bwc DR start")
        bwc_dr = BWC_DR.BWC_DR(
            points,
            window_lenght=CONFIG["WINDOW_LENGTH"],
            limit=CONFIG.as_int("NPOINTS"),
            nys=nys,
        )
        bwc_dr.compress()
        bwc_dr.finalize_trips()
        results["BWC_DR"] = bwc_dr.finalized_trips
        print("BWC_DR finished")

    if "BWC_Squish" in algos:
        print("bwcsquish start")
        bwc_squish = BWC_SQUISH.BWC_SQUISH(
            points,
            window_lenght=CONFIG["WINDOW_LENGTH"],
            limit=CONFIG.as_int("NPOINTS"),
            nys=nys,
        )
        bwc_squish.compress()
        results["BWC_Squish"] = bwc_squish.trips
        print("bwcsquish finished")

    if "BWC_STTrace_Imp" in algos:
        print("bwcsttraceImp start")
        bwc_sttrace_imp = BWC_STTrace_Imp.BWC_STTrace_Imp(
            points,
            window_lenght=CONFIG["WINDOW_LENGTH"],
            limit=CONFIG.as_int("NPOINTS"),
            nys=nys,
            init_trips=trips,
            eval_delta=CONFIG["OPTREG_FREQ"],
        )
        bwc_sttrace_imp.compress()
        results["BWC_STTrace_Imp"] = bwc_sttrace_imp.trips
        print("BWCCSTTRaceImp finished")

    if "BWC_STTrace" in algos:
        print("bwcsttrace start")
        bwc_sttrace = BWC_STTrace.BWC_STTrace(
            points,
            window_lenght=CONFIG["WINDOW_LENGTH"],
            limit=CONFIG.as_int("NPOINTS"),
            nys=nys,
        )
        bwc_sttrace.compress()
        results["BWC_STTrace"] = bwc_sttrace.trips
        print("BWCCSTTRace finished")

    return results


def compile_trips(results, original_trips):
    """Results should be dico[name]:TGeomPointSequence"""

    all_compressed_trajectories = original_trips.rename(
        {"trajectory": "Original"}, axis=1
    )

    for name, trajectory in results.items():
        all_compressed_trajectories = all_compressed_trajectories.join(
            trajectory, how="outer"
        )
        all_compressed_trajectories = all_compressed_trajectories.rename(
            {"trajectory": name}, axis=1
        )

    return all_compressed_trajectories


def assess_results(all_compressed_trajectories, algorithms):
    num_points = {}
    for algo in algorithms:
        print(algo)
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
        precision=CONFIG["EVAL_DELTA"],
    )

    res = compile_results(scores, distances, num_points, algos=algorithms)
    res.to_csv("res/" + CONFIG["testname"] + ".csv", mode="a")
    print()
    print(res)
    return res


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


def compress_and_evaluate(algorithms):
    dbhandler = DBHandler.DBHandler(db=CONFIG_DATA["db"], debug=False)
    trips = dbhandler.load_table(
        table="trips_cleaned", columns=["id", "trajectory"], df_index="id"
    )
    print(len(trips))
    points = dbhandler.load_table(
        table="points_cleaned", columns=CONFIG_DATA["points_columns"]
    )
    points = points.sort_values(by=["point"], ascending=True)
    print(len(points))
    dbhandler.close()

    results = compress_classic(points, trips, algorithms)
    results = compress_bwc(points, trips, algorithms, results=results)

    all_compressed_trajectories = compile_trips(results, trips)

    res = assess_results(all_compressed_trajectories, algorithms)

    return res


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        test = sys.argv[1]

    pymeos_initialize()

    tests = [ "copenhague_15min", "copenhague_2h", "copenhague_60min", "copenhague_5min", "copenhague_1min", "copenhague_30sec",
             "birds_3months_1h",
             "birds_3months_31d",
             "birds_3months_7d",
             "birds_3months_1d",
             "birds_3months_6h"
    ]

    algorithms = [
        "TDTR",
        "DR",
        "Classical_Squish",
        "Classical_STTrace",
        "BWC_Squish",
        "BWC_STTrace",
        "BWC_STTrace_Imp",
        "BWC_DR",
    ]


    # it is not necessary to apply classical algorithms on different datasets!
    algorithms = algorithms[4:]

    all_res = pd.DataFrame(index=algorithms)
    for test in tests:
        print(test)
        CONFIG = ConfigObj("tests_10_percent.ini")
        CONFIG_CLASSIC = CONFIG[CONFIG[test]["classic"]]
        CONFIG_DATA = CONFIG[CONFIG[test]["dataset"]]
        CONFIG = CONFIG[test]

        delta = {CONFIG["WINDOW_SIZE_UNIT"]: CONFIG.as_int("WINDOW_SIZE")}
        CONFIG["WINDOW_LENGTH"] = timedelta(**delta)
        CONFIG["OPTREG_FREQ"] = timedelta(
            **{CONFIG["OPTREG_FREQ_UNIT"]: CONFIG.as_int("OPTREG_FREQ")}
        )

        CONFIG["EVAL_DELTA"] = CONFIG["OPTREG_FREQ"] / 2

        res = compress_and_evaluate(algorithms)
        res = res.rename(columns={"avg_error": test})
        all_res = pd.merge(all_res, res[test], left_index=True, right_index=True)

        print(test)

    print(all_res)
    all_res.to_csv("res/all.csv", mode="a")
