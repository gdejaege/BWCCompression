import pickle
from datetime import timedelta

from pymeos import TGeomPointInst, pymeos_initialize
import shapely as shp
from pyproj import Proj

from bwc.dr_with_anomalies import BWC_DR_Anomaly
from helpers.data_handler import load_csv_to_df, save_df_to_csv
from helpers.utility import convert_trip_points, convert_trips_points
from plotters.plot_with_anomalies import plot_trajectories_to_fig


def preprocess_ais_anomalies(limit=int(1e4)):
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
    print("raw loaded", len(instants))
    instants["point"] = instants.progress_apply(
        lambda row: TGeomPointInst(
            point=shp.Point(row["Longitude"], row["Latitude"]),
            timestamp=row["Timestamp"],
            srid=4326,
        ),
        axis=1,
    )
    instants = instants.sort_values(by="Timestamp").head(limit)
    instants.drop(["Timestamp", "Longitude","Latitude"], axis=1, inplace=True)

    point_counts = instants['id'].value_counts()
    instants = instants[instants['id'].isin(point_counts[point_counts >= 10].index)]

    print("instant", len(instants))
    print("saving")
    save_df_to_csv(dataset, instants, quality="preprocessed")
    print(instants.head())
    print()
    return instants

def explore_data(instants):
    print(instants.head(), len(instants))
    start = instants.iloc[0].point.timestamp()
    end = instants.iloc[-1].point.timestamp()
    # end = max([pt.timestamp() for pt in self.instants["point"]])
    print(start, end)


def plot(trajectories, anomalies):
    return

def analyse_compression(instants, window_size, limit, proj, anomaly_duration):
    compressor = BWC_DR_Anomaly(instants, window_size, limit, proj, anomaly_duration)
    compressor.compress()
    trajectories = compressor.finalized_trips
    anomalies = compressor.anomalies
    # plot_trajectories_to_fig(trajectories, anomalies)

    df = convert_trips_points(trajectories)

    window = "00:20:00"
    save_df_to_csv("ais_anomalies", df, quality="compressed", case="test", algorithm="bwc_dr_anomaly", window=window)

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
    # exit()
    # instants = preprocess_ais_anomalies(limit=int(1e6))
    dataset = "ais_anomalies"
    columns = ["id", "point", "is_anomaly", "sog", "cog"]
    instants = load_csv_to_df(dataset, columns, quality="preprocessed")
    print(instants.head())
    window_size = timedelta(minutes=20)
    limit = 200
    proj =  Proj("EPSG:32632", preserve_units=True)
    anomaly_duration = timedelta(seconds=30)
    analyse_compression(instants, window_size, limit, proj, anomaly_duration)

