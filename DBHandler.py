from pymeos.db.psycopg2 import MobilityDB

import pandas as pd

class DBHandler():

    def __init__(self, db='mobilitydb', debug=False):
        self.connect_db(db)
        self.debug=debug
        return

    def connect_db(self, db='mobilitydb'):
        host = 'localhost'
        port = 25432
        user = 'docker'
        password = 'docker'

        self.connection = MobilityDB.connect(host=host, port=port, database=db, user=user, password=password)
        self.cursor = self.connection.cursor()
        print("connected")

    def create_table(self, table, columns=["mmsi", "point"], columns_types=["integer",  "public.tgeompoint"]):
        columns_str = [columns[i]+" "+columns_types[i] for i in range(len(columns))]
        
        query = f"CREATE TABLE {table} ({', '.join(columns_str)});"
        if self.debug:
            print(query)
        else:
            self.cursor.execute("DROP TABLE IF EXISTS {}  CASCADE;".format(table))
            self.cursor.execute(query)
            self.connection.commit()
        return

    def load_table(self, table, columns=["mmsi", "point"], df_index=None, ntrips=None):
        """Loads table containing points/trips into a dataframe.
        Exmples for trips 20 trips:
        trips = dbhandler.load_table(table="AIS_cleaned", columns=["mmsi", "trajectory"],
                                     df_index="mmsi", ntrips=20)
        """
        columns_string = ', '.join(columns)
        limit_string = "" if ntrips is None else f"LIMIT {ntrips}"
        
        query = f"SELECT {columns_string} FROM {table} {limit_string};"
        
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall(), columns=columns)
        if df_index is not None:
            df.set_index(df_index, inplace=True)
        return df


    def save_df(self, table, df, columns=None, keep_index=True):  
        """
        The dataframe columns are used if columns are not specified
        """
        if columns is None:
            columns = df.columns 

        columns_str = ",".join(columns)
        if keep_index:
            columns_str = df.index.names[0] + ", " + columns_str


        query = f"INSERT INTO {table}({columns_str}) \n VALUES " 

        values = []
        for ind, row in df.iterrows():
            index_str = "'" +str(ind) +"', " if keep_index else ""
            values.append("\n(" + index_str + ",".join(["'"+str(row[col])+"'" for col in columns]) + ")")
            

        query += ",".join(values) +";"

        if self.debug:
            print(query)
        else:
            self.cursor.execute(query)
            self.connection.commit()
        print("saved")


    def rollback(self):
        self.connection.rollback()
        print("rolled back")


    def close(self):   
        self.connection.commit()
        self.cursor.close()

