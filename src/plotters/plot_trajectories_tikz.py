import pyproj
from shapely.geometry import Point


def generate_tikz(original_polyline, simplifications, projection=None):
    # Apply projection if provided
    if projection:
        proj = pyproj.Transformer.from_crs("EPSG:4326", projection, always_xy=True)
        original_polyline = [proj.transform(x, y) for x, y in original_polyline]
        for key in simplifications:
            simplifications[key] = [proj.transform(x, y) for x, y in simplifications[key]]

    tikz_code = r"""
\begin{tikzpicture}[scale=1, every node/.style={circle, draw, fill=black, inner sep=1.5pt}, 
                    line/.style={thick}, faint/.style={gray, thin, dashed}]
    """

    # Draw Original Polyline
    tikz_code += "\n% Original polyline (Top)\n"
    for idx, (x, y) in enumerate(original_polyline):
        tikz_code += f"\\node (P{idx + 1}) at ({x:.2f},{y:.2f}) {{}};\n"

    tikz_code += "\\draw[line] " + " -- ".join([f"(P{idx + 1})" for idx in range(len(original_polyline))]) + ";\n"

    for idx, (x, y) in enumerate(original_polyline):
        tikz_code += f"\\node[below=2pt] at (P{idx + 1}) {{P{idx + 1}}};\n"

    tikz_code += "\\node[above=10pt] at ({:.2f},{:.2f}) {{\\textbf{{Original Polyline}}}};\n".format(
        sum(x for x, y in original_polyline) / len(original_polyline),
        max(y for x, y in original_polyline) + 0.5
    )

    # Draw Simplifications
    for i, (name, simplification) in enumerate(simplifications.items()):
        y_shift = -(2.5 * (i + 1))
        tikz_code += f"\n% {name} (Below Original)\n"
        tikz_code += f"\\begin{{scope}}[yshift={y_shift}cm]\n"

        # Reposition nodes
        for idx, (x, y) in enumerate(original_polyline):
            tikz_code += f"    \\node (S{i}P{idx + 1}) at ({x:.2f},{y:.2f}) {{}};\n"

        # Faint full polyline
        tikz_code += "    \\draw[faint] " + " -- ".join(
            [f"(S{i}P{idx + 1})" for idx in range(len(original_polyline))]) + ";\n"

        # Highlight simplification points
        tikz_code += "    % Highlighted points in simplification\n"
        for idx, point in enumerate(original_polyline):
            if point in simplification:
                tikz_code += f"    \\node[draw=red, fill=red] at (S{i}P{idx + 1}) {{}};\n"

        tikz_code += f"    \\node[above=5pt] at ({sum(x for x, y in original_polyline) / len(original_polyline):.2f}, 0.75) {{\\textbf{{{name}}}}};\n"
        tikz_code += "\\end{scope}\n"

    # Closing TikZ
    tikz_code += r"\end{tikzpicture}"

    return tikz_code


# Example Data
original_polyline = [(12.4924, 41.8902), (12.4964, 41.9028), (12.5000, 41.9100), (12.5100, 41.9200), (12.5200, 41.9300)]
simplifications = {
    "Simplification 1": [(12.4924, 41.8902), (12.5000, 41.9100), (12.5200, 41.9300)],
    "Simplification 2": [(12.4924, 41.8902), (12.5100, 41.9200), (12.5200, 41.9300)]
}

# Example projection: EPSG:32633 (UTM zone 33N, suitable for Rome)
projection = "EPSG:32633"

# Generate TikZ code
tikz_output = generate_tikz(original_polyline, simplifications, projection=projection)

# Write to file
with open("polyline_picture_only.tex", "w") as f:
    f.write(tikz_output)

print("TikZ picture code generated and saved to 'polyline_picture_only.tex'")
