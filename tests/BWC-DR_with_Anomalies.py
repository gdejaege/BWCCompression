import pickle
from datetime import timedelta, datetime

import pandas as pd
from pymeos import TGeomPointInst, pymeos_initialize
import shapely as shp
from pyproj import Proj

from bwc.dr import BWC_DR
from bwc.dr_anomaly import BWC_DR_anomaly_new
from helpers.data_handler import load_csv_to_df, save_df_to_csv
from helpers.utility import convert_trip_points, convert_trips_points, convert_points_trips
from plotters.plot_with_anomalies import plot_trajectories_to_fig

def raw_ais_anomalies_line_from_compression(start=None, stop=None):
    dataset = "ais_anomalies"
    fname = "data/raw/ais_anomalies/points.csv"
    instants = pd.read_csv(fname, header=0)
    print(len(instants))
    print(instants.head())
    instants["Timestamp"] = pd.to_datetime(instants["timestamp"])
    instants = instants.drop_duplicates(subset=['mmsi', 'Timestamp'], keep='first')
    print("timestamps transformed")
    instants = instants[(instants["Timestamp"] >= start) & (instants["Timestamp"] <= stop)]
    print(len(instants))

    dataset = "ais_anomalies_24h"
    algorithms = ["BWC_DR_anomaly_new", "BWC_DR"]
    compression_ratios = [0.1, 0.25, 0.5]
    # compression_ratios = [0.1, 0.25]
    window = "0:00:30"
    print("compressed:")
    for compression_ratio in compression_ratios:
        for algo in algorithms:
            points = load_csv_to_df(dataset, algorithm=algo, columns=["id", "point"], quality="compressed", compression_ratio=compression_ratio, window=window)
            if algo == "BWC_DR_anomaly_new":
                algo = "BWC_DR_anomaly"
            output_name = "res/anomalies/" + str(compression_ratio).replace(".","_") + "_" + algo + "_raw.csv"
            print(len(points))
            res = raw_of_points(instants, points)
            res.to_csv(output_name)
            print(output_name, instants["is_anomaly"].sum(), res["is_anomaly"].sum())

def raw_of_points(full, compressed):
    compressed["mmsi"] = compressed["id"]
    compressed["Timestamp"] = pd.to_datetime(compressed["point"].progress_apply(lambda p: p.timestamp())).dt.tz_localize(None)
    compressed.drop(["id", "point"], axis=1, inplace=True)
    result = full.merge(compressed, on=["mmsi", "Timestamp"], how="inner")
    # print(len(result), len(compressed))
    result.drop(["Timestamp"], axis=1, inplace=True)
    return result


def preprocess_ais_anomalies(start=None, stop=None):
    RENAME_COLS = {
        "mmsi": "id",
        "timestamp": "Timestamp",
        "SOG": "sog",
        "COG": "cog",
        "time": "Timestamp",
        "icao24": "id",
        "lat": "Latitude",
        "lon": "Longitude",
        "velocity": "sog",
        "knots": "sog",
        "heading": "cog",
    }
    print("preprocessing anomalies")
    dataset = "ais_anomalies"
    columns = ["mmsi", "timestamp", "lon", "lat", "is_anomaly", "knots", "cog"]
    instants = load_csv_to_df(dataset, columns, process=False, names_transform=RENAME_COLS)
    print(len(instants))
    instants["Timestamp"] = pd.to_datetime(instants["Timestamp"])
    print("timestamps transformed")
    instants = instants[(instants["Timestamp"] >= start) & (instants["Timestamp"] <= stop)]
    print("raw loaded", len(instants))
    instants = instants.drop_duplicates(subset=['id', 'Timestamp'], keep='first')

    print("duplicates dropped loaded", len(instants))
    instants["point"] = instants.progress_apply(
        lambda row: TGeomPointInst(
            point=shp.Point(row["Longitude"], row["Latitude"]),
            timestamp=row["Timestamp"],
            srid=4326,
        ),
        axis=1,
    )
    instants = instants.sort_values(by="Timestamp")
    instants.drop(["Timestamp", "Longitude","Latitude"], axis=1, inplace=True)

    # point_counts = instants['id'].value_counts()
    # instants = instants[instants['id'].isin(point_counts[point_counts >= 10].index)]

    print("instant", len(instants))
    print("saving")
    save_df_to_csv(dataset, instants, quality="preprocessed")
    print(instants.head())
    print()
    trips = convert_points_trips(instants)
    print("trips", len(trips))
    return instants

def explore_data(instants):
    print(instants.head(), len(instants))
    start = instants.iloc[0].point.timestamp()
    end = instants.iloc[-1].point.timestamp()
    end = max([pt.timestamp() for pt in self.instants["point"]])
    print(start, end)


def plot(trajectories, anomalies):
    return

def analyse_compression(instants, window_size, limit, proj, anomaly_duration):
    compressor = BWC_DR_anomaly_new(instants, window_size, limit, proj, anomaly_duration)
    compressor.compress()
    trajectories = compressor.finalized_trips
    anomalies = compressor.anomalies
    # plot_trajectories_to_fig(trajectories, anomalies)

    df = convert_trips_points(trajectories)

    window = "00:20:00"
    save_df_to_csv("ais_anomalies_24h", df, quality="compressed", case="test", algorithm="bwc_dr_anomaly", window=window)

    with open('res/anomalies.json', 'wb') as f:
            pickle.dump(anomalies, f)
    print("finished")

def test_sorted_list():
    from sortedcontainers import SortedList
    l = SortedList()
    l.add(1)
    l.add(3)
    l.add(2)
    print(l)


if __name__ == "__main__":
    pymeos_initialize()
    start = datetime(year=2018, month=7, day=3, hour=0, minute=0, second=0, microsecond=0)
    stop = datetime(year=2018, month=7, day=4)
    raw_ais_anomalies_line_from_compression(start, stop)
    exit()
    instants = preprocess_ais_anomalies(start, stop)
    dataset = "ais_anomalies_24h"
    columns = ["id", "point", "is_anomaly", "sog", "cog"]
    instants = load_csv_to_df(dataset, columns, quality="preprocessed")
    print(instants.head())
    print(len(instants))
    timestamps = [pt.timestamp() for pt in instants["point"]]
    print(min(timestamps), max(timestamps))
    # window_size = timedelta(minutes=20)
    # limit = 200
    # proj =  Proj("EPSG:32632", preserve_units=True)
    # anomaly_duration = timedelta(seconds=30)
    # analyse_compression(instants, window_size, limit, proj, anomaly_duration)

