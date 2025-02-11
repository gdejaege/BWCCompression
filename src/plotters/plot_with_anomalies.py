import json
import pickle

from matplotlib import pyplot as plt
from pymeos import pymeos_initialize

from helpers.data_handler import load_csv_to_df
from helpers.utility import convert_points_trips


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

if __name__ == "__main__":
    pymeos_initialize()
    dataset = "ais_anomalies"
    window = "00:20:00"
    compressed_points = load_csv_to_df("ais_anomalies", columns=["id", "point"], quality="compressed", case="test", algorithm="bwc_dr_anomaly", window=window)
    compressed_trajectories = convert_points_trips(compressed_points)

    with open('res/anomalies.json', 'rb') as f:
        anomalies = pickle.load(f)
    print(anomalies)
    plot_trajectories_to_fig(compressed_trajectories, anomalies)

