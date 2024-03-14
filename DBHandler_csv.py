from pymeos import *
import os

import pandas as pd


class DBHandler():
    """In case of problems with MobilityDB, emulate DB with CSV files."""

    def __init__(self, db='mobilitydb', debug=False):
        self.db=db
        return


    def load_table(self, table, columns: list[str] =["id", "point"], df_index=None, ntrips=None):
        """Loads table containing points/trips into a dataframe.
        Exmples for trips 20 trips:
        trips = dbhandler.load_table(table="AIS_cleaned", columns=["mmsi", "trajectory"],
                                     df_index="mmsi", ntrips=20)
        """
        fn = "data/"+self.db+"/"+table
        df = pd.read_csv(fn)

        if df_index is not None:
            df.set_index(df_index, inplace=True)

        
        if 'trajectory' in df.columns:
            df['trajectory'] = df['trajectory'].apply(lambda x: TGeomPointSeq(x))

        if 'point' in df.columns:
            df['point'] = df['point'].apply(lambda x: TGeomPointInst(x))
        # print(df.head())
        
        return df

    def close(self):   
        return


def save_trips(trips, state, dataset):
    filename = dataset +"/trips_"+ state
    trips.to_csv(filename)


def save(points, trips, db, idtype, points_columns, points_columns_types, state=""):
    if state != "":
        state = "_"+state

    folder = "data/"+db+"/"
    if not os.path.exists(folder):
        os.makedirs(folder)

    fn = folder + "trips" + state
    trips.to_csv(fn)
    print("trips saved")
    fn = folder + "points" + state
    points.to_csv(fn)

    print("points saved")



