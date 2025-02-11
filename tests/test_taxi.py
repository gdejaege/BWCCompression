from pymeos import pymeos_initialize, TGeomPointInst

from bwc.squish import BWC_SQUISH
from compress import compress
from helpers.data_handler import load_config, load_csv_to_df
from helpers.utility import convert_points_trips

if __name__ == "__main__":
    pymeos_initialize()
    dataset = "taxi"
    compression_ratio = 0.1

    cfg = load_config(dataset, compression_ratio)
    windows = cfg["windows"]
    limits = cfg["points"]
    proj = cfg["proj"]
    points = load_csv_to_df(dataset, cfg["columns"], quality="preprocessed")
    print("points loaded", len(points))
    trips = convert_points_trips(points)  # create trips here
    print("trips converted", len(trips))
    cfg["trips"] = trips

    print("compressing")
    bwc = BWC_SQUISH(points, windows[0], limits[0], proj)
    bwc.compress()




