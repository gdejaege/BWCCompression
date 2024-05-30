import pandas as pd
from tqdm import tqdm
tqdm.pandas()
import os
from pymeos import TGeomPointInst

RAW = "data/raw/"
PREPROCESSED = "data/preprocessed/"

def load_csv_to_df(dataset, columns, names_transform=None, preprocessed=True):
    fname = filename(dataset, preprocessed)
    instants = pd.read_csv(fname, header=0, usecols=columns)

    if names_transform is not None:
        instants = instants.rename(names_transform, axis=1)

    instants['point'] = instants.progress_apply(lambda row: TGeomPointInst(row.point),
                                    axis=1)
    instants = instants.dropna()
    return instants


def filename(dataset, preprocessed) -> str:
    if preprocessed:
        folder = PREPROCESSED + dataset + "/"
        if not os.path.exists(folder):
            os.makedirs(folder)
        fname = folder + "points.csv"
    else:
        fname = RAW + dataset + "/points.csv"
    return fname

def save_df_to_csv(dataset_name, df, preprocessed=True):
    out_fname = filename(dataset_name, preprocessed)
    df.to_csv(out_fname)
