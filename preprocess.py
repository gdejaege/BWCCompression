import pandas as pd
from tqdm import tqdm
tqdm.pandas()
from configobj import ConfigObj

from pymeos.db.psycopg2 import MobilityDB
from pymeos import *

import DBHandler 
from utility import *

import geopandas as gpd
from geopandas import GeoDataFrame, read_file
import movingpandas as mpd

import warnings
warnings.filterwarnings('ignore')
# mpd.show_versions()


def load_csv_to_df():
    raw = pd.read_csv(CONFIG["fname"], header=0, usecols=CONFIG["csv_columns"])

    names_transform = {"MMSI":"id", "location-long": "Longitude", "location-lat":"Latitude", "timestamp":"Timestamp", 
            "individual-local-identifier":"id", "SOG":"sog", "COG":"cog"}

    raw = raw.rename(names_transform, axis=1)
    if CONFIG["dataset"] == "birds":
        print("here")
        raw['Timestamp'] = raw.progress_apply(lambda row: datetime.strptime(row['Timestamp']+"+02:00", "%Y-%m-%d %H:%M:%S.%f%z"),
                                            axis=1)
    raw = raw.dropna()
    return raw


def construct_instants(raw):
    """Remove uncessary columns and create sequences."""    
    df = raw.copy()
    df['point'] = df.progress_apply(lambda row: TGeomPointInst(point=shp.Point(row['Longitude'], row['Latitude']), 
                                                             timestamp=row['Timestamp'], srid=CONFIG['srid']),
                                    axis=1)
    df.drop(['Latitude', 'Longitude', 'Timestamp'], axis=1, inplace=True)
    return df

def filter_points_period(points):
    period = Period('('+CONFIG["period"]["start"]+', '+ CONFIG["period"]["end"]+')')
    points_index = points['point'].progress_map(lambda point: point.is_temporally_contained_in(period))
    return points[points_index]

def filter_points_tbox(points):
    TL = shp.set_srid(shp.Point(*CONFIG["box"]["TL"]), CONFIG["srid"])
    BR = shp.set_srid(shp.Point(*CONFIG["box"]["BR"]), CONFIG["srid"])
    box = STBox.from_geometry_time(shp.union(TL, BR), Period(lower='2021-01-01', upper='2021-01-02', upper_inc=True))
    points_index = points['point'].progress_map(lambda point: point.ever_intersects(box))
    return points[points_index]
    

def compute_trips(ais):
    def detect_short_tris(ais):
        small_trips = ais.groupby('id').size()
        return small_trips.loc[small_trips <= 1]
    
    trips = ais.loc[~ais['id'].isin(detect_short_tris(ais).index)].groupby('id').aggregate(
        {
            'point': TGeomPointSeq.from_instants,
        }
    ).rename({'point': 'trajectory'}, axis=1)
    
    return trips


# Clean with moving pandas 
def clean_trips_with_mpd(trip, vmax):
    traj = trip.trajectory.to_dataframe()
    # traj.index = pd.to_datetime(traj.index, utc=True) # needs utc for some unkown reason of pandas?
    mpd_traj = mpd.Trajectory(traj, 1)
    mpd_traj.add_speed(overwrite=True)
    
    cleaned = mpd.OutlierCleaner(mpd_traj).clean(v_max=vmax)    # what does alpha do ?

    wkt = "SRID=4326;" + extract_wkt_from_traj(cleaned)
    return TGeomPointSeq(string=wkt, normalize=False)


def clean_all_trips(init_trips, cleaning_strategy=clean_trips_with_mpd):
    trips = init_trips.copy()
    # print(trips.head())
    # trips_index = init_trips['trajectory'].progress_map(lambda traj: len(traj.instants())>0)
    cleaning_strategy_l = lambda x: cleaning_strategy(x, vmax=CONFIG["clean"].as_int("vmax"))
    trips['trajectory'] = trips.progress_apply(cleaning_strategy_l, axis=1)
    return trips   


def raw_points_from_clean_trips(trips_cleaned, raw_points):
    id_ts = {ind: [instant.timestamp() for instant in row.trajectory.instants()] for ind, row in trips_cleaned.iterrows()}
    points = [point for _,point in raw_points.iterrows() if point.point.timestamp() in id_ts.get(point.id, [])]
    return pd.DataFrame(points, columns=["id", "point", "sog", "cog"]) # raw_points[poins_filtered_index]


def filter_outliers(points, outliers):
    points_index = points['id'].progress_map(lambda x: x not in outliers)
    return points[points_index]


def preprocess(load=False):
    if not load:
        raw_points = load_csv_to_df()
        print("points loaded")
        points = construct_instants(raw_points)
        print(points.head())
        if "outliers" in CONFIG:
            points = filter_outliers(points, [int(x) for x in CONFIG["outliers"]])
        if 'filter_periond' in CONFIG and CONFIG.as_bool('filter_period'):
            points = filter_points_period(points)
        if CONFIG.as_bool('filter'):
            points = filter_points_tbox(points)
        trips = compute_trips(points)

        DBHandler.save(points, trips, CONFIG["db"],
                       points_columns=CONFIG["points_columns"], 
                       points_columns_types=CONFIG["points_columns_types"], 
                       idtype=CONFIG["idtype"])

        # trips.set_index("id", inplace=True)
    else:
        dbhandler = DBHandler.DBHandler(db=CONFIG["db"], debug=False)
        trips = dbhandler.load_table(table="trips", columns=["id", "trajectory"], df_index="id")
        points = dbhandler.load_table(table="points", columns=CONFIG["points_columns"])
        dbhandler.close()

    print(len(points))
    trips_clean = clean_all_trips(trips)
    points_clean = raw_points_from_clean_trips(trips_clean, points)
    DBHandler.save(points_clean, trips_clean, CONFIG["db"], state="cleaned", 
            points_columns=CONFIG["points_columns"], 
            points_columns_types=CONFIG["points_columns_types"], idtype=CONFIG["idtype"])


    

if __name__ == "__main__":
    test = "birds_3months"
    test = "copenhague2"

    pymeos_initialize()
    CONFIG = ConfigObj("test_config.ini")
    CONFIG = CONFIG[test]

    preprocess(load=False)



