import matplotlib.pyplot as plt


import pandas as pd


def deserialize_trajectory(cursor, connection, table, temporal_column, static_column=None):
    """
    Add a column to the table containing a non temporal geometry representing a 
    temporal trajectory.
    """
    # to do
    pass


# Type conversions
def convert_points_trips(points):
    """
    Agregate a dataframe with TGeomInst to TGeomSeq
    """
    trajectories = points.groupby('mmsi').aggregate(
        {
            'point': TGeomPointSeq.from_instants
        }
    ).rename({'point': 'trajectory'}, axis=1)
    return trajectories


def convert_trips_points(trips, sort=True):
    """
    Dissagregate a dataframe with TGeomSeq to TGeomInst
    """
    point_generator = ((mmsi, instant) for mmsi, row in trips.iterrows() for instant in row.trajectory.instants())

    points = pd.DataFrame.from_records(point_generator, columns = ["mmsi", "point"])
    if sort:
        # print(points.head())
        points = points.sort_values(by="point") # , key=lambda x: x.str.lower())
    return points



# Plotting

def plot_trips(sequences, colors=[]):
    """

    """
    fig, axes = plt.subplots()

    sequences = [sequences] if type(sequences) != list else sequences

    for i, s in enumerate(sequences):
        if len(colors) > i:
            s.plot(color=colors[i])
        else:
            s.plot()
    
    plt.show()

