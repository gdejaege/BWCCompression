import pprint
from collections import defaultdict
from pathlib import Path

import pandas as pd

from pymeos import pymeos_initialize

import sys
import os

from bwc.STTraceImp import BWC_STTrace_Imp
from bwc.STTraceImp_delay import BWC_STTrace_Imp_Delay
from bwc.dr import BWC_DR
from bwc.dr_anomaly import BWC_DR_anomaly_new
from bwc.dr_with_anomalies import BWC_DR_Anomaly
from bwc.random import BWC_Random
from bwc.squish import BWC_SQUISH
from bwc.squish_delay import BWC_SQUISH_Delay
from bwc.sttrace import BWC_STTrace
from bwc.sttrace_delay import BWC_STTrace_Delay
from bwc.uniform import BWC_uniform
from helpers.data_handler import load_compressed_trajectories
from helpers.metrics import SED_trips, length_loss_rate, synchronized_speed_difference

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.helpers.data_handler import load_csv_to_df, load_config
from src.helpers.utility import (convert_points_trips,)


CONFIG_PATH = "tests/config_compress.ini"

def assess_bwc_algorithms(datasets, compression_ratios, algorithms, metrics):
    for dataset in datasets:
        print("\n", dataset)
        all_points = load_csv_to_df(dataset, ["id", "point"], quality="preprocessed")
        init_trips = convert_points_trips(all_points)
        print(all_points.head())
        print(init_trips.head())

        for compression_ratio in compression_ratios:
            scores = defaultdict(dict)
            case_cfg = load_config(dataset, compression_ratio)
            eval_delta = case_cfg["eval_delta"]
            proj = case_cfg["proj"]

            for algorithm in algorithms:
                print("Assessing", algorithm)
                for metric in metrics:
                    scores[metric][algorithm.__name__] = defaultdict(dict)

                for window_index in range(len(case_cfg["windows"])):
                    window_name = str(case_cfg["windows"][window_index])
                    compressed_points = load_compressed_trajectories(algorithm, case_cfg, window_index)
                    compressed_trips = convert_points_trips(compressed_points)
                    score_algo_window = assess_compressed_trips(compressed_trips, init_trips, eval_delta, metrics, proj)
                    print("score", algorithm.__name__, case_cfg["windows"][window_index], score_algo_window)
                    for metric in score_algo_window:
                        scores[metric][algorithm.__name__][window_name] = score_algo_window[metric]

            pprint.pprint(scores)

            save_scores(scores, dataset, compression_ratio)
    return

def save_scores(scores, dataset, compression_ratio):
    for metric in scores:
        scores_m = scores[metric]
        df = pd.DataFrame.from_dict(scores_m, orient="index")
        print(df.head())
        folder = "res/" + metric + "/"
        Path(folder).mkdir(parents=True, exist_ok=True)
        fn = dataset + str(compression_ratio).replace(",","_") + ".csv"
        df.to_csv(folder+fn, mode="a")
        # all_res.to_csv("res/bwc_compression/all.csv", mode="a")

def assess_compressed_trips(compressed_trips, init_trips, eval_delta, metrics, proj):
    scores = {}
    for metric in metrics:
        print(metric)
        if metric == "SED":
            scores[metric] = SED_trips(init_trips, compressed_trips, eval_delta)
        elif metric == "LLR":
            scores[metric] = length_loss_rate(init_trips, compressed_trips)
        elif metric == "SSD":
            scores[metric] = synchronized_speed_difference(init_trips, compressed_trips, eval_delta, proj)
    return scores

if __name__ == "__main__":
    pymeos_initialize()
    datasets = ["ais", "birds", "flights", "taxi"]
    datasets = ["ais", "birds", "flights"]
    datasets = ["ais_anomalies"]
    algorithms = [
        BWC_Random,
        BWC_uniform,
        BWC_SQUISH,
        BWC_SQUISH_Delay,
        BWC_STTrace,
        BWC_STTrace_Delay,
        BWC_STTrace_Imp,
        BWC_STTrace_Imp_Delay,
        BWC_DR,
    ]
    algorithms = [BWC_DR_anomaly_new, BWC_DR]
    compression_ratios = [x/1000 for x in range(100, 300, 25)][1:]
    metrics = ["SSD", "LLR", "SED"]
    assess_bwc_algorithms(datasets, compression_ratios, algorithms, metrics)
