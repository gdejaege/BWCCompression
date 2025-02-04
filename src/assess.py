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
from bwc.random import BWC_Random
from bwc.squish import BWC_SQUISH
from bwc.squish_delay import BWC_SQUISH_Delay
from bwc.sttrace import BWC_STTrace
from bwc.sttrace_delay import BWC_STTrace_Delay
from helpers.data_handler import load_compressed_trajectories
from helpers.metrics import SED_trips, length_loss_rate, synchronized_speed_difference

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.helpers.data_handler import load_csv_to_df, load_config
from src.helpers.utility import (convert_points_trips,)


CONFIG_PATH = "tests/config_compress.ini"

def assess_bwc_algorithms(datasets, compression_ratios, algorithms, metrics):
    for dataset in datasets:
        all_points = load_csv_to_df(dataset, ["id", "point"], quality="preprocessed")
        init_trips = convert_points_trips(all_points)

        for compression_ratio in compression_ratios:
            scores = defaultdict(dict)
            case_cfg = load_config(dataset, compression_ratio)
            eval_delta = case_cfg["eval_delta"]

            for algorithm in algorithms:
                for metric in metrics:
                    scores[metric][algorithm.__name__] = defaultdict(dict)

                for window_index in range(len(case_cfg["windows"])):
                    window_name = str(case_cfg["windows"][window_index])
                    compressed_points = load_compressed_trajectories(algorithm, case_cfg, window_index)
                    compressed_trips = convert_points_trips(compressed_points)
                    score_algo_window = assess_compressed_points(compressed_trips, init_trips, eval_delta, metrics)
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

def assess_compressed_points(compressed_points, init_points, eval_delta, metrics):
    scores = {}
    for metric in metrics:
        if metric == "SED":
            scores[metric] = SED_trips(init_points, compressed_points, eval_delta)
        elif metric == "LLR":
            scores[metric] = length_loss_rate(init_points, compressed_points)
        elif metric == "SSD":
            scores[metric] = synchronized_speed_difference(init_points, compressed_points)
    return scores

if __name__ == "__main__":
    pymeos_initialize()
    datasets = ["ais"]
    algorithms = [
        BWC_STTrace_Imp,
        BWC_STTrace_Imp_Delay,
        BWC_SQUISH,
        BWC_SQUISH_Delay,
        BWC_STTrace,
        BWC_STTrace_Delay,
        BWC_DR,
        BWC_Random,
    ]
    compression_ratios = [0.1]
    metrics = ["SED", "LLR", "SSD"]
    assess_bwc_algorithms(datasets, compression_ratios, algorithms, metrics)
