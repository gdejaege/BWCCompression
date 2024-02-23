
def create_histograms(algorithms):
    """Function needed only for creating the histograms in the paper."""

    def hist_dico(trajectories, windows):
        res = {}
        for trajectory in trajectories:
            for instant in trajectory.instants():
                time = instant.timestamp()
                for i in range(len(windows) - 1):
                    if windows[i] < time <= windows[i + 1]:
                        res[windows[i]] = res.get(windows[i], 0) + 1
        return res

    def plot_histogram(hist, limit, name):
        plt.figure(figsize=(8, 6), dpi=80)

        labels = list(sorted(hist.keys()))
        labels = [str(x.time())[0:5] for x in labels]
        values = [hist[k] for k in sorted(hist.keys())]

        plt.bar(labels, values, color="gray")
        y = [limit for i in labels]
        plt.plot(labels, y, "--", color="b")
        data = values
        groupings = labels
        x_pos = [i for i, _ in enumerate(groupings)]
        plt.bar(x_pos, data, color="gray")
        plt.xticks(x_pos[::4], groupings[::4], rotation="vertical")
        ticks = range(0, 201, 25)
        labels = [
            str(tick) for tick in ticks
        ]  # Convert to string if you want custom labels
        plt.yticks(ticks, labels)
        
        # Adding titles to the axes
        plt.xlabel('Time Windows')
        plt.ylabel('Number or points')
    
        format = "png"
        plt.savefig(name + "." + format, format=format)
        plt.show()
        # input()

    import datetime
    import matplotlib.pyplot as plt


    dbhandler = DBHandler.DBHandler(db=CONFIG_DATA["db"], debug=False)
    trips = dbhandler.load_table(
        table="trips_cleaned", columns=["id", "trajectory"], df_index="id"
    )
    points = dbhandler.load_table(
        table="points_cleaned", columns=CONFIG_DATA["points_columns"]
    )
    points = points.sort_values(by=["point"], ascending=True)
    dbhandler.close()
    delta = CONFIG["WINDOW_LENGTH"]

    # results = compress_all(points, trips, algorithms)
    results = compress_classic(points, trips, algorithms)

    trips = compile_trips(results, trips)

    # start = hardcoded for ais
    t = start = datetime.datetime.fromisoformat("2021-01-01 00:00:00+01:00")
    end = datetime.datetime.fromisoformat("2021-01-02 00:00:00+01:00")
    windows = []

    while t <= end:
        windows.append(t)
        t += delta

    histograms = {algo: hist_dico(trips[algo], windows) for algo in algorithms}

    for algorithm in algorithms:
        print(algorithm)
        for key, v in histograms[algorithm].items():
            print(key, v)

        plot_histogram(histograms[algorithm], CONFIG.as_float("NPOINTS"), algorithm)



