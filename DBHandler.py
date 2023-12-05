from pymeos.db.psycopg2 import MobilityDB

import pandas as pd

class DBHandler():

    def __init__(self, db='mobilitydb', debug=False):
        self.db=db
        self.connect_db()
        self.debug=debug
        return

    def connect_db(self):
        host = 'localhost'
        port = 25432
        user = 'docker'
        password = 'docker'

        self.connection = MobilityDB.connect(host=host, port=port, database=self.db, user=user, password=password)
        self.cursor = self.connection.cursor()
        # print("connected")

    def create_table(self, table, columns=["id", "point"], columns_types=["integer",  "public.tgeompoint"], key=None):
        columns_str = [columns[i]+" "+columns_types[i] for i in range(len(columns))]

        if key is not None:
            print("primary key")
            columns_str[columns.index(key)] += " Primary Key"
        
        query = f"CREATE TABLE {table} ({', '.join(columns_str)});"
        if self.debug:
            print(query)
        else:
            # print(query)
            self.cursor.execute("DROP TABLE IF EXISTS {}  CASCADE;".format(table))
            self.connection.commit()
            # print("table dropped")
            self.cursor.execute(query)
            # for i, t in enumerate(columns_types):
            #     if t in ('public.tgeompoint', ):
            #         query = "SELECT setSRID({}, {}) from {}".format(columns[i], srid, table)
            #         self.cursor.execute(query)
            self.connection.commit()
        return


    def create_save_df(self, table, df, 
                       columns=["id", "point"], 
                       columns_types=["integer",  "public.tgeompoint"],
                       index=None):  

        self.create_table(table, columns, columns_types, key=index)
        keep_index = index is not None 
        self.save_df(table, df, columns, keep_index=keep_index) 


    def save_df(self, table, df, columns=None, keep_index=True):  
        """
        The dataframe columns are used if columns are not specified
        """

        if columns is None:
            columns = df.columns 

        columns_str = ",".join(columns)

        if keep_index:
            if df.index.name in columns:
                columns.remove(df.index.name)
            else:
                columns_str = df.index.name + ", " + columns_str

        query = f"INSERT INTO {table}({columns_str}) \n VALUES " 
        # print(query)

        values = []
        for ind, row in df.iterrows():
            # print(ind)
            index_str = "'" +str(ind) +"', " if keep_index else ""
            values.append("\n(" + index_str + ",".join(["'"+str(row[col])+"'" for col in columns]) + ")")
            

        query += ",".join(values) +";"

        if self.debug:
            print(query)
        else:
            self.cursor.execute(f"TRUNCATE TABLE {table};")
            self.connection.commit()
            self.cursor.execute(query)
            self.connection.commit()
        # print("saved")


    def load_table(self, table, columns=["id", "point"], df_index=None, ntrips=None):
        """Loads table containing points/trips into a dataframe.
        Exmples for trips 20 trips:
        trips = dbhandler.load_table(table="AIS_cleaned", columns=["mmsi", "trajectory"],
                                     df_index="mmsi", ntrips=20)
        """
        columns_string = ', '.join(columns) if columns != "*" else "*"
        limit_string = "" if ntrips is None else f"LIMIT {ntrips}"
        
        query = f"SELECT {columns_string} FROM {table} {limit_string};"
        
        self.cursor.execute(query)
        df = pd.DataFrame(self.cursor.fetchall(), columns=columns)
        if df_index is not None:
            df.set_index(df_index, inplace=True)
        return df



    def rollback(self):
        self.connection.rollback()
        print("rolled back")


    def close(self):   
        self.connection.commit()
        self.cursor.close()

