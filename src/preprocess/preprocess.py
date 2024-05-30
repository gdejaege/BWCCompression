from pymeos import TGeomPointInst, TGeomPointSeq, STBox
import shapely as shp
import pandas as pd

from src.helpers.utility import extract_wkt_from_traj

import movingpandas as mpd

import warnings
warnings.filterwarnings('ignore')

from tqdm import tqdm
tqdm.pandas()

def construct_instants(raw, srid):
    """Remove uncessary columns and create sequences."""    
    df = raw.copy()
    df['point'] = df.progress_apply(lambda row: TGeomPointInst(point=shp.Point(row['Longitude'], row['Latitude']), 
                                                             timestamp=row['Timestamp'], srid=srid),
                                    axis=1)
    df.drop(['Latitude', 'Longitude', 'Timestamp'], axis=1, inplace=True)
    return df

def filter_points_period(points, period):
    points_index = points['point'].progress_map(lambda point: point.is_temporally_contained_in(period))
    return points[points_index]

def filter_points_tbox(points, box):
    # TL = shp.set_srid(shp.Point(*box["TL"]), srid)
    # BR = shp.set_srid(shp.Point(*box["BR"]), srid)
    points_index = points['point'].progress_map(lambda point: point.ever_intersects(box))
    return points[points_index]


def filter_outliers(points, outliers):
    points_index = points['id'].progress_map(lambda x: x not in outliers)
    return points[points_index]




# Clean with moving pandas 
def clean_trips_with_mpd(trip, vmax):
    traj = trip.trajectory.to_dataframe()

    mpd_traj = mpd.Trajectory(traj, 1)
    mpd_traj.add_speed(overwrite=True)
    
    cleaned = mpd.OutlierCleaner(mpd_traj).clean(v_max=vmax)    # what does alpha do ?

    wkt = "SRID=4326;" + extract_wkt_from_traj(cleaned)
    return TGeomPointSeq(string=wkt, normalize=False)


def clean_all_trips(init_trips, vmax):
    trips = init_trips.copy()
    cleaning_strategy_l = lambda x: clean_trips_with_mpd(x, vmax)
    trips['trajectory'] = trips.progress_apply(cleaning_strategy_l, axis=1)
    return trips   


def raw_points_from_clean_trips(trips_cleaned, raw_points):
    id_ts = {ind: [instant.timestamp() for instant in row.trajectory.instants()] for ind, row in trips_cleaned.iterrows()}
    points = [point for _,point in raw_points.iterrows() if point.point.timestamp() in id_ts.get(point.id, [])]
    return pd.DataFrame(points) # raw_points[poins_filtered_index]
