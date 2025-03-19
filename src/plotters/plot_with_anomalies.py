import json
import pickle
from datetime import timedelta

from matplotlib import pyplot as plt
from pymeos import pymeos_initialize

from helpers.data_handler import load_csv_to_df, load_config
from helpers.utility import convert_points_trips, filter_args, convert_trips_points
from plotters.plot_trajectories_tikz import project_points, get_scaling_factors, apply_scaling


def plot_trajectories_to_fig(trajectories, anomalies, output_file="trajectories_plot.png", delta=2):
    plt.figure(figsize=(8, 4))

    # Plot original polyline
    for id, trajectory in trajectories.iterrows():
        print(id, trajectory)
        polyline = [i.value() for i in trajectory["trajectory"].instants()]
        x_coords, y_coords = zip(*[(pt.x, pt.y) for pt in polyline])
        plt.plot(x_coords, y_coords, color='black', linewidth=2, marker='o')
        for trajectory_anomaly in anomalies.get(id, []):
            print(trajectory_anomaly)
            anomaly_point = trajectory["trajectory"].value_at_timestamp(trajectory_anomaly)
            plt.plot(anomaly_point.x, anomaly_point.y, color='red', linewidth=2, marker='o')

    plt.axis("off")
    plt.savefig(output_file)
    plt.show()
    plt.close()
    print(f"Trajectory plot saved to '{output_file}'")

def load_trajectory(dataset, id, projection, case_algorithm_windows, pt_range):
    init_points_all = load_csv_to_df(dataset, ["id", "point"], quality="preprocessed")
    init_points_traj = init_points_all[init_points_all["id"] == id]["point"].tolist()[pt_range[0]:pt_range[1]]
    print(len(init_points_traj))
    last_point = init_points_traj[-1]
    start = init_points_traj[0].timestamp()
    end = init_points_traj[-1].timestamp()
    print(start, end)

    init_points_traj.sort(key=lambda x: x.timestamp())
    init_points_traj = [pt.value() for pt in init_points_traj]
    init_points_traj = project_points(init_points_traj, projection)
    scaling_factors = get_scaling_factors(init_points_traj)
    init_points_traj = apply_scaling(init_points_traj, scaling_factors)

    compressions = {}

    compression_ratio = 0.3
    cfg = load_config(dataset, compression_ratio)
    points = load_csv_to_df(dataset, cfg["columns"], quality="preprocessed")
    points = points[points["id"] == id]
    print(points.head())
    trips = convert_points_trips(points)
    cfg["trips"] = trips
    cfg["bwc_sttrace_delta"] = timedelta(seconds=30)
    cfg["window_length"] = timedelta(minutes=7)
    cfg["limit"] = 7

    for name, case_algorithm_window in case_algorithm_windows.items():
        case, algo_name, window, algo = case_algorithm_window
        # print(name, case, algo, window)
        params = filter_args(algo, cfg)
        compressor = algo(points,  **params)
        compressor.compress()

        compressed_points = convert_trips_points(compressor.finalized_trips)
        # print(compressed_points.head())
        compressed_points = compressed_points["point"].tolist()



        compressed_points = [pt.value() for pt in compressed_points if pt.timestamp() >= start and pt.timestamp() <= end]
        compressed_points.append(last_point.value())
        compressed_points = project_points(compressed_points, projection)
        compressed_points = apply_scaling(compressed_points, scaling_factors)
        print(name, len(compressed_points))
        compressions[name] = compressed_points

    return init_points_traj, compressions



if __name__ == "__main__":
    pymeos_initialize()
    dataset = "ais_anomalies_24h"
    window = "00:20:00"
    compressed_points = load_csv_to_df("ais_anomalies_24h", columns=["id", "point"], quality="compressed", case="test", algorithm="bwc_dr_anomaly", window=window)
    compressed_trajectories = convert_points_trips(compressed_points)

    with open('res/anomalies.json', 'rb') as f:
        anomalies = pickle.load(f)
    print(anomalies)
    plot_trajectories_to_fig(compressed_trajectories, anomalies)

