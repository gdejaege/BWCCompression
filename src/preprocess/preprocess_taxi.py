from pymeos import TGeomPointInst, pymeos_initialize

import shapely as shp
from helpers.data_handler import filename, load_csv_to_df, save_df_to_csv
from helpers.utility import convert_points_trips
from src.preprocess.preprocess import *

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def preprocess_taxi():
    print("preprocessing Taxi")
    in_dataset = "taxi"
    dataset = "taxi_2"
    out_fname = filename(dataset, quality="preprocessed")
    columns = ["id", "timestamp", "point"]
    instants = load_csv_to_df(in_dataset, columns, process=False)
    print("raw loaded", len(instants))
    # instants = instants[instants["id"] < 128]
    print("id filtered ", len(instants))
    instants['timestamp'] = pd.to_datetime(instants['timestamp'], format="ISO8601")
    print("raw timestamp converted", len(instants))
    instants = instants[instants["timestamp"] < datetime(2014, 2, 2, tzinfo=pytz.FixedOffset(60))]
    print("raw filtered", len(instants))
    instants["point"] = instants.progress_apply(
        lambda row: TGeomPointInst(
            point=shp.Point(map(float, row["point"][7:-1].split())),
            timestamp=row["timestamp"],
            srid=4326,
        ),
        axis=1,
    )
    instants.drop(["timestamp"], axis=1, inplace=True)

    trips = convert_points_trips(instants)

    trips_clean = filter_short(trips, 15)
    print("trips:", len(trips_clean))
    trips_clean = clean_all_trips(trips_clean, vmax=100, units=("km", "h"))
    print("trips:", len(trips_clean))
    instants_clean = raw_points_from_clean_trips(trips_clean, instants)
    print("instant", len(instants_clean))
    instants_clean["Timestamp"] = instants_clean["point"].apply(
        lambda point: point.timestamp()
    )
    instants_clean_sorted = instants_clean.sort_values(by="Timestamp")
    print("saving")
    save_df_to_csv(dataset, instants_clean_sorted, quality="preprocessed")
    print(instants_clean_sorted.head())
    print()
    return


if __name__ == "__main__":
    pymeos_initialize()
    preprocess_taxi()
