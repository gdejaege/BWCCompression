from datetime import timedelta

import pyproj
from bokeh.io import output_file
from matplotlib import pyplot as plt
from pymeos import pymeos_initialize
from shapely.affinity import rotate
from shapely.geometry import Point

from bwc.STTraceImp import BWC_STTrace_Imp
from bwc.STTraceImp_delay import BWC_STTrace_Imp_Delay
from bwc.dr import BWC_DR
from bwc.random import BWC_Random
from bwc.squish import BWC_SQUISH
from bwc.squish_delay import BWC_SQUISH_Delay
from bwc.sttrace import BWC_STTrace
from bwc.sttrace_delay import BWC_STTrace_Delay
from bwc.uniform import BWC_uniform
from helpers.data_handler import load_csv_to_df, load_config
from helpers.utility import filter_args, convert_trips_points, convert_points_trips


def get_scaling_factors(points, target_x_range=(0, 10), target_y_range=(0, 5)):
    """
    Extracts scaling factors from a list of Shapely Points to transform them
    into a specified target range for both x and y.

    Args:
    - points: List of Shapely Point objects
    - target_x_range: Tuple of the form (min_x, max_x) for scaling the x values.
    - target_y_range: Tuple of the form (min_y, max_y) for scaling the y values.

    Returns:
    - scaling_factors: A tuple of (x_scale, x_offset, y_scale, y_offset)
    """
    x_values, y_values = zip(*[(point.x, point.y) for point in points])
    min_x, max_x = min(x_values), max(x_values)
    min_y, max_y = min(y_values), max(y_values)

    x_scale = (target_x_range[1] - target_x_range[0]) / (max_x - min_x) if max_x != min_x else 1
    y_scale = (target_y_range[1] - target_y_range[0]) / (max_y - min_y) if max_y != min_y else 1

    x_offset = target_x_range[0] - min_x * x_scale
    y_offset = target_y_range[0] - min_y * y_scale

    return (x_scale, x_offset, y_scale, y_offset)


def apply_scaling(points, scaling_factors):
    """
    Scales a list of Shapely Points using the provided scaling factors.

    Args:
    - points: List of Shapely Point objects
    - scaling_factors: A tuple of (x_scale, x_offset, y_scale, y_offset)

    Returns:
    - scaled_points: A list of scaled Shapely Point objects
    """
    x_scale, x_offset, y_scale, y_offset = scaling_factors

    scaled_points = [
        Point(point.x * x_scale + x_offset, point.y * y_scale + y_offset)
        for point in points
    ]

    return scaled_points

def plot_trajectories(original_polyline, simplifications):
    return

def generate_tikz(original_polyline, simplifications):
    # Apply projection if provided
    tikz_code = r"""
\begin{tikzpicture}[scale=1, every node/.style={circle, draw, fill=black, inner sep=1.5pt}, 
                    line/.style={thick}, faint/.style={gray, thin, dashed}]
    """

    # Draw Original Polyline
    tikz_code += "\n% Original polyline (Top)\n"
    for idx, pt in enumerate(original_polyline):
        x, y = pt.x, pt.y
        tikz_code += f"\\node (P{idx + 1}) at ({x:.2f},{y:.2f}) {{}};\n"

    tikz_code += "\\draw[line] " + " -- ".join([f"(P{idx + 1})" for idx in range(len(original_polyline))]) + ";\n"

    for idx, pt in enumerate(original_polyline):
        x, y = pt.x, pt.y
        tikz_code += f"\\node[below=2pt] at (P{idx + 1}) {{P{idx + 1}}};\n"

    tikz_code += "\\node[above=10pt] at ({:.2f},{:.2f}) {{\\textbf{{Original Polyline}}}};\n".format(
        sum(pt.x for pt in original_polyline) / len(original_polyline),
        max(pt.y for pt in original_polyline) + 0.5
    )

    # Draw Simplifications
    for i, (name, simplification) in enumerate(simplifications.items()):
        y_shift = -(2.5 * (i + 1))
        tikz_code += f"\n% {name} (Below Original)\n"
        # tikz_code += f"\\begin{{scope}}[yshift={y_shift}cm]\n"

        # Reposition nodes
        for idx, pt in enumerate(original_polyline):
            x, y = pt.x, pt.y
            tikz_code += f"    \\node (S{i}P{idx + 1}) at ({x:.2f},{y:.2f}) {{}};\n"

        # Faint full polyline
        # tikz_code += "    \\draw[faint] " + " -- ".join(
        #     [f"(S{i}P{idx + 1})" for idx in range(len(original_polyline))]) + ";\n"

        # Highlight simplification points
        tikz_code += "    % Highlighted points in simplification\n"
        for idx, point in enumerate(original_polyline):
            if point in simplification:
                tikz_code += f"    \\node[draw=red, fill=red] at (S{i}P{idx + 1}) {{}};\n"

        tikz_code += f"    \\node[above=5pt] at ({sum(pt.x for pt in original_polyline) / len(original_polyline):.2f}, 0.75) {{\\textbf{{{name}}}}};\n"
        tikz_code += "\\end{scope}\n"

    # Closing TikZ
    tikz_code += r"\end{tikzpicture}"

    return tikz_code

def project_points(points, projection):
    proj = pyproj.Transformer.from_crs(pyproj.CRS("EPSG:4326"), projection, always_xy=True)
    points = [Point(proj.transform(point.x, point.y)) for point in points]
    print("projected")
    return points


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
    cfg["limit"] = 20

    for name, case_algorithm_window in case_algorithm_windows.items():
        case, algo_name, window, algo = case_algorithm_window
        # print(name, case, algo, window)
        old = False
        if old:
            compressed_points = load_csv_to_df(dataset, ["id", "point"], quality="compressed", case=case,
                                               algorithm=algo, window=window)

            compressed_points = compressed_points[compressed_points["id"] == id]["point"].tolist()
        else:
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


def plot_trajectories_to_fig(original_polyline, simplifications, output_file="trajectories_plot.png", delta=2):
    # For the maris UC4 paper draing, change here
    plt.figure(figsize=(16, 8))

    angle = 20  # Change this value to rotate more/less
    origin = (4.35, 50.85)  # Rotation origin (center of rotation)

    original_polyline = [rotate(pt, angle, origin=origin) for pt in original_polyline]

    # Plot original polyline
    x_coords, y_coords = zip(*[(pt.x, pt.y) for pt in original_polyline])

    plt.plot(x_coords, y_coords, label='Original', color='black', linewidth=2, marker='o')
    #plt.text(10.5, 1, "Original", fontsize=14, verticalalignment='top', horizontalalignment='left')

    # Plot simplifications
    i = 1
    for name, simplification in simplifications.items():
        simplification = [rotate(pt, angle, origin=origin) for pt in simplification]
        x_simpl, y_simpl = zip(*[(pt.x, pt.y-(i*delta)) for pt in simplification])
        i += 1
        plt.plot(x_simpl, y_simpl, markersize=12, label=name, linewidth=1.5, linestyle='', marker='.', color='red')

    legend = plt.legend()
    legend.remove()
    # plt.xlabel('X Coordinate')
    # plt.ylabel('Y Coordinate')
    # plt.title('Polyline and Simplifications')
    # plt.grid(True)
    plt.axis("off")
    plt.savefig(output_file)
    plt.show()
    plt.close()
    print(f"Trajectory plot saved to '{output_file}'")



if __name__ == "__main__":
    # Example Data
    pymeos_initialize()
    dataset = "ais_2"
    id = 219026706
    projection = pyproj.CRS("EPSG:32633")
    pt_range = (5000, 5500)
    case_algorithm_windows = {
        #"BWC-Random": ("ais_0_1", "BWC-Random", "0:15:00", BWC_Random),
        #"BWC-Uniform": ("ais_0_1", "BWC-Uniform", "0:15:00", BWC_uniform),
        #"BWC-Squish": ("ais_0_1", "BWC-Squish", "0:15:00", BWC_SQUISH),
        #"BWC-Squish-Delay": ("ais_0_1", "BWC-Squish-Delay", "0:15:00", BWC_SQUISH_Delay),
        #"BWC-STTrace": ("ais_0_1", "BWC-STTrace", "0:15:00", BWC_STTrace),
        #"BWC-STTrace-Delay": ("ais_0_1", "BWC-STTrace-Delay", "0:15:00", BWC_STTrace_Delay),
        #"BWC-STTrace-Imp": ("ais_0_1", "BWC-STTrace-Imp", "0:15:00", BWC_STTrace_Imp),
        #"BWC-STTrace-Imp-Delay": ("ais_0_1", "BWC-STTrace-Imp-Delay", "0:15:00", BWC_STTrace_Imp_Delay),
        "BWC-DR": ("ais_0_1", "BWC-DR", "0:15:00", BWC_DR),
    }
    full, compressed = load_trajectory(dataset, id, projection, case_algorithm_windows, pt_range)
    # print(len(full), len(compressed["BWC-DR"]))

    # Generate TikZ code
    output_file = "illustration.png"
    plot_trajectories_to_fig(full, compressed, output_file, delta=0)
    # tikz_output = generate_tikz(full, compressed)

    # Write to file

    # with open("/home/gilles/Documents/Mobispace/articles/bwc/src/test.tex", "w") as f:
    #    f.write(tikz_output)
    # print(len(init_points_traj))



    # print("TikZ picture code generated and saved to 'polyline_picture_only.tex'")
