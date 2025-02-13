"""
Script Name: data_handler.py

Description: Save and load the different datasets (raw/preprocessed/compressed)

Naming Conventions:
- dataset: the original data to be compressed (ex: AIS, Birds, ...).
- quality: weither the dataset is raw, preprocessed or compressed.
- case: a dataset and a compression ratio (ex: AIS_10, for 10 percent of AIS).
- window: for algorithms working with time windows, a duration of that window. 
          The windows are described as "subconfigurations" of a case.

File Paths:
datasets are located under the path:
data/quality/dataset/[case/algorithm/][window/]points.csv
"""
from datetime import timedelta

import pandas as pd
from tqdm import tqdm

from data.constants import CONFIG_PATH, DATASETS_CONFIGS

tqdm.pandas()
import os
from pymeos import TGeomPointInst
from pyproj import Proj
from configobj import ConfigObj

RAW = "data/raw/"
PREPROCESSED = "data/preprocessed/"
DATA = "data/"

def load_csv_to_df(dataset, columns, quality="raw", case="", algorithm="", window="", names_transform=None, process=True):
    fname = filename(dataset, quality, case, algorithm, window)
    # print("loading:", fname)
    instants = pd.read_csv(fname, header=0, usecols=columns)

    if names_transform is not None:
        instants = instants.rename(names_transform, axis=1)

    if 'point' in instants.columns and process:
        instants['point'] = instants.apply(lambda row: TGeomPointInst(row.point),
                                    axis=1)
    instants = instants.dropna()
    return instants


def filename(dataset, quality="raw", case="", algorithm="", window="") -> str:
    folder = DATA + quality + "/" + dataset + "/"
    if case:
        folder += case + "/" + algorithm + "/"
        if window:
            folder += window + "/"

    if not os.path.exists(folder):
        os.makedirs(folder)
    fname = folder + "points.csv"
    return fname

def save_df_to_csv(dataset_name, df, quality="raw", case="", algorithm="", window=""):
    out_fname = filename(dataset_name, quality, case, algorithm, window)
    print("saving to :", out_fname)
    df.to_csv(out_fname)


def save_compressed_trajectories(algo, compressed_points, cfg):
    save_df_to_csv(
        dataset_name=cfg["dataset"],
        df=compressed_points,
        quality="compressed",
        case=cfg["case"],
        algorithm=algo.__name__,
        window=str(cfg.get("window_length", "")),
    )
    return

def load_compressed_trajectories(algo, case_cfg, window_index):
    df = load_csv_to_df(
        dataset=case_cfg["dataset"],
        columns=["id", "point"],
        quality="compressed",
        case=case_cfg["case"],
        algorithm=algo.__name__,
        window=str(case_cfg["windows"][window_index]),
    )
    return df



def load_config(dataset, compression_ratio):
    """Return configuration for the different cases and their windows.

    One dict for the algorithms without time windowing + one dict per window of the case.
    """
    conf_file = DATASETS_CONFIGS+"/"+dataset+".ini"
    CONFIG = ConfigObj(conf_file)
    cfg = {}
    # print(conf_file, CONFIG)

    time_unit = CONFIG["UNIT"]
    cfg["case"] = dataset + "_" + str(compression_ratio).replace(".", "_")
    cfg["dataset"] = dataset
    cfg["columns"] = CONFIG["COLUMNS"]  # type: ignore
    cfg["proj"] = Proj(CONFIG["PROJ"], preserve_units=True)  # type: ignore

    cfg["bwc_sttrace_delta"] = timedelta(
        **{time_unit: CONFIG.as_int("OPTREG_FREQ")}  # type: ignore
    )

    cfg["eval_delta"] = cfg["bwc_sttrace_delta"] / 2

    cfg["windows"] = [timedelta(**{time_unit: float(window_size)}) for window_size in CONFIG["WINDOWS"]]
    cfg["points"] = [int(int(x)*compression_ratio*10) for x in CONFIG["POINTS_WINDOWS"]]

    if "ANOMALY_THRESHOLD" in CONFIG:
        cfg["anomaly_threshold"] = timedelta(**{time_unit: CONFIG.as_int("ANOMALY_THRESHOLD")})

    return cfg


