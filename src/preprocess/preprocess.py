from pandas.core.common import random_state
from pymeos import TGeomPointInst, TGeomPointSeq, STBox
import shapely as shp
import pandas as pd

from datetime import datetime

import pytz

from src.helpers.utility import extract_wkt_from_traj

import movingpandas as mpd

import warnings

import random

warnings.filterwarnings("ignore")

from tqdm import tqdm

tqdm.pandas()


def construct_instants(raw, srid):
    """Remove uncessary columns and create sequences."""
    df = raw.copy()
    df["point"] = df.progress_apply(
        lambda row: TGeomPointInst(
            point=shp.Point(row["Longitude"], row["Latitude"]),
            timestamp=row["Timestamp"],
            srid=srid,
        ),
        axis=1,
    )
    df.drop(["Latitude", "Longitude", "Timestamp"], axis=1, inplace=True)
    return df


def is_valid_timestamp(ts):
    try:
        # Try to convert the timestamp to a datetime object
        datetime.fromtimestamp(int(ts))
        return True
    except (ValueError, OSError):
        return False


def convert_timestamp(ts):
    try:
        # print(datetime.fromtimestamp(int(ts)))
        return datetime.fromtimestamp(int(ts))
    except (ValueError, OSError, KeyError) as e:
        print(f"Error converting row with timestamp '{ts}': {e}")
        return pd.NaT  # Use pd.NaT for invalid timestamps


def build_datetime(points):
    points["Timestamp"] = points["Timestamp"].progress_apply(convert_timestamp)
    return points.dropna()


def filter_points_period(points, period):
    points_index = points["point"].progress_map(
        lambda point: point.is_temporally_contained_in(period)
    )
    return points[points_index]


def filter_trips_stbox(trips, box):
    trips_index = trips["trajectory"].progress_map(lambda traj: traj.is_contained_in(box))
    return trips[trips_index]


def filter_points_stbox(points, box):
    points_index = points["point"].progress_map(
        lambda point: point.ever_intersects(box)
    )
    return points[points_index]


def filter_short(trips, mn):
    trips = trips[trips["trajectory"].apply(lambda traj: len(traj.instants()) > mn)]
    return trips


def max_trips(trips, mx):
    mx = min(mx, len(trips))
    return trips.sample(n=mx, random_state=0)


def filter_outliers(points, outliers):
    points_index = points["id"].progress_map(lambda x: x not in outliers)
    return points[points_index]


def filter_ids(points, ids):
    return points[points["id"].isin(ids)]


# Clean with moving pandas
def clean_trips_with_mpd(trip, vmax, units):
    traj = trip.trajectory.to_dataframe()

    mpd_traj = mpd.Trajectory(traj, 1)
    mpd_traj.add_speed(overwrite=True)

    if units is None:
        cleaned = mpd.OutlierCleaner(mpd_traj).clean(v_max=vmax)  # what does alpha do ?
    else:
        cleaned = mpd.OutlierCleaner(mpd_traj).clean(v_max=vmax, units=units)

    wkt = "SRID=4326;" + extract_wkt_from_traj(cleaned)
    return TGeomPointSeq(string=wkt, normalize=False)


def clean_all_trips(init_trips, vmax, units=None):
    """Using geo-pandas"""
    trips = init_trips.copy()
    cleaning_strategy_l = lambda x: clean_trips_with_mpd(x, vmax, units)
    trips["trajectory"] = trips.progress_apply(cleaning_strategy_l, axis=1)
    return trips


def raw_points_from_clean_trips(trips_cleaned, raw_points):
    id_ts = {
        ind: [instant.timestamp() for instant in row.trajectory.instants()]
        for ind, row in trips_cleaned.iterrows()
    }
    points = [
        point
        for _, point in raw_points.iterrows()
        if point.point.timestamp() in id_ts.get(point.id, [])
    ]
    return pd.DataFrame(points)  # raw_points[poins_filtered_index]


def construct_absolute_time(df, timezone):
    tz = pytz.timezone(timezone)
    df["point"] = df.progress_apply(
        lambda row: tz.localize(
            datetime.strptime(row["Timestamp"], "%Y-%m-%d %H:%M:%S.%f")
        ).strftime("%Y-%m-%d %H:%M:%S.%f%z"),
        axis=1,
    )

    return df
