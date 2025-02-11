from pymeos import pymeos_initialize, TGeomPointInst, TGeomPointSeq

from helpers.data_handler import load_csv_to_df
from helpers.utility import convert_points_trips

from shapely.ops import transform
from pyproj import Proj
from shapely.geometry import Point


# Convert each geometry point
def reproject_tgeom(trajectory, proj):
    transformed_instants = []
    for instant in trajectory.instants():
        point = instant.value()  # Extract Shapely Point
        # new_point = transform(transformer.transform, point)  # Reproject
        new_point = Point(*proj(point.x, point.y))
        print(new_point)
        transformed_instants.append(TGeomPointInst(point=new_point, timestamp=instant.timestamp()))

    return TGeomPointSeq(instant_list=transformed_instants)




if __name__ == "__main__":
    pymeos_initialize()
    dataset = "ais"
    compression_ratios = [0.1]
    columns = ["id", "point"]
    proj = Proj("EPSG:32632", preserve_units=True)
    all_points = load_csv_to_df(dataset, columns, quality="preprocessed")
    trips = convert_points_trips(all_points)
    print(trips.head())

    traj = trips.loc[209525000]["trajectory"]
    traj2 = reproject_tgeom(traj, proj)
    print(traj)
    print(traj2)

    timestamps = [i.timestamp() for i in traj.instants()]
    length = traj.cumulative_length()
    length2 = traj2.cumulative_length()
    lengths = [length.value_at_timestamp(ts) for ts in timestamps[:3]]
    lengths2 = [length2.value_at_timestamp(ts) for ts in timestamps[:3]]
    for l, l2 in zip(lengths, lengths2):
        print(l, l2)

