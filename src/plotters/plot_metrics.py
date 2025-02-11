import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import timedelta

import matplotlib.pyplot as plt
import numpy as np
import tikzplotlib


def plot_csv_data(file_path, out_fn, limits=None):
    y_min, y_max  = None, None
    if limits is not None:
        y_min, y_max = limits
    # Load CSV (first column as index)
    df = pd.read_csv(file_path, index_col=0)
    print(file_path)
    print(df.head())
    print()

    # Convert duration strings (hh:mm:ss) to timedelta
    x_values = [pd.to_timedelta(t) for t in df.columns]

    # Convert timedelta to seconds for logarithmic scale
    x_seconds = np.array([t.total_seconds() for t in x_values])

    # Plot each row
    plt.figure(figsize=(8, 5))
    for idx, row in df.iterrows():
        plt.plot(x_seconds, row, marker='o', label=idx)

    # Set x-axis to log scale
    plt.xscale("log")

    # Format x-ticks (convert seconds to readable time)
    ax = plt.gca()
    ax.set_xticks(x_seconds)
    ax.set_xticklabels([str(timedelta(seconds=int(t))) for t in x_seconds], rotation=45)

    # Labels and title
    # plt.xlabel("Duration (hh:mm:ss)")
    plt.ylabel("Value")
    # plt.title("CSV Data Plot (Semi-Log X-Axis)")

    # Set y-axis limits if specified
    if y_min is not None and y_max is not None:
        plt.ylim(y_min, y_max)

    plt.grid(True, which="both", linestyle="--", alpha=0.6)
    legend = plt.legend()
    legend.remove()  # This prevents TikZ from misinterpreting the legend
    # tikzplotlib.save(out_fn)
    plt.show()

def res_fn(dataset, case, metric):
    return "res/"+metric+"/"+dataset +str(case) +".csv"

if __name__ == "__main__":
    datasets = ["taxi"]
    datasets = ["ais", "birds", "flights", "taxi"]
    dataset = "taxi_2"
    case = "0.1"
    metric = "SSD"
    metrics = ["SED", "LLR", "SSD"]
    limits = [(0, 15), (0, 0.2), (0, 0.2)] # AIS limits
    limits = [(0, 5000), (0, 0.5), (0, 0.4)] # birds limit
    limits = [(0, 20), (0, 5500), (0, 5000), (0, 200)] # limits SED
    limits = [(0, 0.15), (0.15, 0.4), (0, 0.15), (0, 0.2)] # limits LLR
    limits = [(0.04, 0.16), (0.08, 0.4), (5, 15), (0, 0.2)] # limits SSD
    limits = [(0, 1000), (0, 0.7), (0, 2)] # taxis

    for metric, limit in zip(metrics, limits):
        fn = res_fn(dataset, case, metric)
        output_file = "/home/gilles/Documents/Mobispace/articles/bwc/src/" + metric+"_"+dataset +"_"+str(case)+".tex"
        print(output_file)
        plot_csv_data(fn, output_file, limit)
