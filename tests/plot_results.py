import matplotlib.pyplot as plt
import numpy as np
import tikzplotlib

import webcolors
# print(webcolors.CSS3_NAMES_TO_HEX)  # Instead of CSS3_HEX_TO_NAMES

# Given x-values (time in seconds) and corresponding y-values
x = np.array([30, 7200, 1200, 300, 3600])  # Time

# Evaluation results (unordered x-values)
data = {
    "BWC_DR": [11.18, 13.75, 14.23, 13.39, 15.75],
    "BWC_STTrace": [91.55, 4.91, 4.31, 5.97, 4.40],
    "BWC_STTrace_Delay": [8.95, 5.91, 6.54, 4.81, 5.32],
    "BWC_STTrace_Imp": [91.55, 1.49, 1.72, 4.63, 1.53],
    "BWC_STTrace_Imp_Delay": [7.48, 1.47, 1.54, 1.71, 1.50],
    "BWC_Squish": [136.87, 10.86, 7.33, 7.89, 10.61],
    "BWC_Squish_Delay": [11.61, 10.83, 7.38, 7.79, 10.67]
}

# Sort x values and reorder y-values accordingly
sorted_indices = np.argsort(x)
x_sorted = x[sorted_indices]

# Sort each method's y-values based on sorted x
sorted_data = {key: np.array(values)[sorted_indices] for key, values in data.items()}

# Plot log-log
plt.figure(figsize=(8,6))
for label, y_values in sorted_data.items():
    plt.semilogx(x_sorted, y_values, marker='o', linestyle='-', label=label)

plt.xlabel("Time (seconds, log scale)")
plt.ylabel("Evaluation (meters, log scale)")
plt.grid(True, which="both", linestyle="--", alpha=0.6)
plt.title("Evaluation of Different Systems Over Time (Log-Log Scale)")

legend = plt.legend()
legend.remove()  # This prevents TikZ from misinterpreting the legend
tikzplotlib.save("res/plot.tex")
plt.show()
