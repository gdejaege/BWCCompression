import pytest
import os
from pymeos import pymeos_initialize, STBox
from src.preprocess.preprocess import *
from src.helpers.utility import compute_trips
from src.helpers.data_loader import load_csv_to_df, save_df_to_csv
from src.helpers.data_loader import filename


RENAME_COLS = {
    "MMSI": "id",
    "location-long": "Longitude",
    "location-lat": "Latitude",
    "timestamp": "Timestamp",
    "individual-local-identifier": "id",
    "SOG": "sog",
    "COG": "cog",
}


@pytest.fixture(scope="session", autouse=True)
def preprocess_datasets():
    pymeos_initialize()
    datasets = [
        {  # AIS SAMPLE
            "name": "ais_sample",
            "srid": 4326,
            "raw_columns": ("Timestamp", "MMSI", "Latitude", "Longitude", "SOG", "COG"),
            "vmax": 30,
        },
        {  # BIRDS
            "name": "birds",
            "srid": 4326,
            "raw_columns": (
                "timestamp",
                "location-long",
                "location-lat",
                # "tag-local-identifier",
                "individual-local-identifier",
            ),
            "vmax": 30,
            "filter_period": STBox(
                tmin="2021-07-09 00:00:00+02:00",
                tmax="2021-10-09 00:00:00+02:00",
                tmax_inc=True,
            ),
        },
        {  # AIS Full
            "name": "ais_20210101",
            "srid": 4326,
            "raw_columns": ("Timestamp", "MMSI", "Latitude", "Longitude", "SOG", "COG"),
            "filter_stbox": STBox(
                xmin=12.47,
                ymax=55.75,
                xmax=13.08,
                ymin=55.48,
                tmin="2021-01-01+01:00",
                tmax="2021-01-02+01:00",
                srid=4326
            ),
            "vmax": 25,
            "recompute": False,
            "outliers": [111219514],
        },
    ]

    for dataset in datasets:
        preprocess(dataset)


def preprocess(dataset):
    print(dataset["name"])
    out_fname = filename(dataset["name"], preprocessed=True)
    if not os.path.exists(out_fname) or dataset.get("recompute", False):
        raw = load_csv_to_df(dataset["name"], dataset["raw_columns"], RENAME_COLS, preprocessed=False)
        instants = construct_instants(raw, dataset["srid"])
        # no filtering for ais_sample
        if "outlier" in dataset:
            instants = filter_outliers(instants, dataset["outliers"])
        if "filter_period" in dataset:
            instants = filter_points_period(instants, dataset["filter_period"])
        if "filter_stbox" in dataset:
            instants = filter_points_tbox(instants, dataset["filter_stbox"])
        
        print("filtered:", len(instants))
        

        trips = compute_trips(instants)
        print("trips:", len(trips))
        trips_clean = clean_all_trips(trips, vmax=dataset["vmax"])
        instants_clean = raw_points_from_clean_trips(trips_clean, instants)
        save_df_to_csv(dataset["name"], instants_clean)
        print(len(instants_clean))
    else:
        print(dataset["name"])
        instants = load_csv_to_df(dataset["name"], columns=("id", "point"))
        # print(len(instants))
    return 1


