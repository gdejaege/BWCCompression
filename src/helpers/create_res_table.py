from datetime import timedelta

import pandas as pd

def generate_latex_table(csv_file, output_file, time_unit="seconds", ds="ais"):
    # Read CSV file
    df = pd.read_csv(csv_file, index_col=0)
    print(df.head())

    # Mapping of algorithm names to LaTeX commands
    algorithm_mapping = {
        "BWC_Random": "\\random",
        "BWC_uniform": "\\uniform",
        "BWC_SQUISH": "\\bwcsquish",
        "BWC_SQUISH_Delay": "\\delaybwcsquish",
        "BWC_STTrace": "\\bwcsttrace",
        "BWC_STTrace_Delay": "\\delaybwcsttrace",
        "BWC_STTrace_Imp": "\\bwcsttraceopt",
        "BWC_STTrace_Imp_Delay": "\\delaybwcsttraceopt",
        "BWC_DR": "\\bwcdr",
    }

    # Extract window sizes and convert them to seconds
    window_sizes = [int(timedelta(seconds=int(t)).total_seconds()) for t in df.columns]
    print(window_sizes)

    # LaTeX table header
    latex_table = "\\begin{table}[H]\n\\begin{tabular}{l " + " r" * len(window_sizes) + "}\n"
    latex_table += "\\toprule \n window size (seconds) & {} \\\\ \n".format( " & ".join(map(str, window_sizes)))
    latex_table += " points per window &" + " & ".join(["x"] * len(window_sizes)) + "\\\\"
    latex_table += "\n\\midrule"""

    # Add data rows
    for algorithm, row in df.iterrows():
        if time_unit == "seconds":
            latex_table += "{} & {} \\\\\n".format(algorithm_mapping.get(algorithm, algorithm),
                                             " & ".join(str(int(v)) for v in row))
        if time_unit == "minutes":
            latex_table += "{} & {} \\\\\n".format(algorithm_mapping.get(algorithm, algorithm),
                                                   " & ".join(str(int(v/60)) for v in row))
        if time_unit == "hours":
            latex_table += "{} & {} \\\\\n".format(algorithm_mapping.get(algorithm, algorithm),
                                                   " & ".join(str(int(v/3600)) for v in row))


    # LaTeX table footer
    latex_table += """\n\\bottomrule \n\end{tabular}\n\caption{Average delay of the different \\bwc algorithms on the AIS dataset for different sizes of time windows.}"""
    latex_table += "\n%\label{tab:delays_"+ds+"_10}\n\end{table}"

    # Write to file
    print(latex_table)
    with open(output_file, "w") as f:
        f.write(latex_table)

    print(f"LaTeX table saved to {output_file}")


if __name__ == "__main__":
    output = "temp"
    ds = "ais"
    input_f = "res/delays/ais_0_1/all.csv"
    output_f = "/home/gilles/Documents/Mobispace/articles/bwc/src/ais_delays.tex"
    generate_latex_table(input_f, output_f, time_unit="seconds", ds=ds)
    ds = "flights"
    input_f = "res/delays/flights_0_1/all.csv"
    output_f = "/home/gilles/Documents/Mobispace/articles/bwc/src/flights_delays.tex"
    generate_latex_table(input_f, output_f, time_unit="seconds", ds=ds)
    ds = "birds"
    input_f = "res/delays/flights_0_1/all.csv"
    input_f = "res/delays/birds_0_1/all.csv"
    output_f = "/home/gilles/Documents/Mobispace/articles/bwc/src/birds_delays.tex"
    generate_latex_table(input_f, output_f, time_unit="minutes", ds=ds)
    ds = "taxis"
    input_f = "res/delays/taxi_2_0_1/all.csv"
    output_f = "/home/gilles/Documents/Mobispace/articles/bwc/src/taxi_delays.tex"
    generate_latex_table(input_f, output_f, time_unit="minutes", ds=ds)
    # birds in hours
    # taxi_0_2 in minutes

