"""

@TODO: Since I will probably not paralelize the code, I should change the name of this file

"""

from _pytest.outcomes import Failed
import pytest
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pymeos import pymeos_initialize, STBox
from src.preprocess.preprocess import *
from src.helpers.utility import convert_points_trips
from src.helpers.data_handler import load_csv_to_df, save_df_to_csv
from src.helpers.data_handler import filename


RENAME_COLS = {
    "MMSI": "id",
    "location-long": "Longitude",
    "location-lat": "Latitude",
    "timestamp": "Timestamp",
    "individual-local-identifier": "id",
    "SOG": "sog",
    "COG": "cog",
    "time": "Timestamp",
    "icao24": "id",
    "lat": "Latitude",
    "lon": "Longitude",
    "velocity": "sog",
    "heading": "cog",
}


# @pytest.fixture(scope="session", autouse=True)
def test_preprocess():
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
            "timezone": "Europe/Brussels",
            "recompute": False,
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
                srid=4326,
            ),
            "vmax": 25,
            "recompute": False,
            "outliers": [111219514],
        },
        {  # Flights 6h
            "name": "flights",
            "srid": 4326,
            "raw_columns": ("time", "icao24", "lat", "lon", "velocity", "heading"),
            "vmax": 600,
            "recompute": True,
            "timestamp": True,
            "min_lenght": 100,
            # "max_trips": 1000,
            "filter_stbox_trips": STBox(
                xmin=-16.1,
                ymax=84.73,
                xmax=40.18,
                ymin=32.88,
                tmin="2020-05-30+01:00",
                tmax="2020-06-02+01:00",
                srid=4326,
            ),
        },
    ]

    for dataset in datasets:
        preprocess(dataset)


def preprocess(dataset):
    print("preprocessing", dataset["name"])
    out_fname = filename(dataset["name"], quality="preprocessed")
    if not os.path.exists(out_fname) or dataset.get("recompute", False):
        raw = load_csv_to_df(
            dataset["name"], dataset["raw_columns"], names_transform=RENAME_COLS
        )
        print("raw loaded", len(raw))
        # print(raw.head(20))
        if dataset.get("timestamp"):
            print("building datetimes")
            raw = build_datetime(raw)
        if "timezone" in dataset:
            raw = construct_absolute_time(raw, dataset["timezone"])
        # no filtering for ais_sample
        if "outliers" in dataset:
            print("outlier removal")
            raw = filter_outliers(raw, dataset["outliers"])
        if "filter_ids" in dataset:
            print("filtering ids")
            raw = filter_ids(raw, dataset["filter_ids"])
        instants = construct_instants(raw, dataset["srid"])
        if "filter_period" in dataset:
            print("filtering period")
            instants = filter_points_period(instants, dataset["filter_period"])
        if "filter_stbox" in dataset:
            print("filtering stbox")
            instants = filter_points_stbox(instants, dataset["filter_stbox"])

        print("filtered:", len(instants))

        trips = convert_points_trips(instants)
        print("trips:", len(trips))
        if "filter_stbox_trips" in dataset:
            trips = filter_trips_stbox(trips, dataset["filter_stbox_trips"])
            print("trips filtered:", len(trips))
        trips_clean = clean_all_trips(trips, vmax=dataset["vmax"])



        if "min_lenght" in dataset:
            trips_clean = filter_short(trips_clean, dataset["min_lenght"])
        if "max_trips" in dataset:
            trips_clean = max_trips(trips_clean, dataset["max_trips"])
        print("trips:", len(trips))
        instants_clean = raw_points_from_clean_trips(trips_clean, instants)
        print("instant", len(instants_clean))
        instants_clean["Timestamp"] = instants_clean["point"].apply(
            lambda point: point.timestamp()
        )
        instants_clean_sorted = instants_clean.sort_values(by="Timestamp")
        print("saving")
        save_df_to_csv(dataset["name"], instants_clean_sorted, quality="preprocessed")
        print()
    return


if __name__ == "__main__":
    test_preprocess()
