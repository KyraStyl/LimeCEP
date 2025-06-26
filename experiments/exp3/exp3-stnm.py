import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import matplotlib.colors as mcolors

# Grouped data as nested dictionaries
grouped_data = {
    "ABC": {
        "SASE":     {"#MATCHES": 14, "FP": 6, "FN": 0, "TP": 8, "RECALL": 100, "PRECISION": 57.14},
        "SASEXT":   {"#MATCHES": 40, "FP": 25, "FN": 0, "TP": 15, "RECALL": 100, "PRECISION": 37.5},
        "FLINKCEP": {"#MATCHES": 14, "FP": 6, "FN": 0, "TP": 8, "RECALL": 100, "PRECISION": 57.14},
        "LIMECEP":  {"#MATCHES": 8,  "FP": 0, "FN": 0, "TP": 8, "RECALL": 100, "PRECISION": 100.0}
    },
    "AB+C": {
        "SASE":     {"#MATCHES": 34, "FP": 22, "FN": 0, "TP": 12, "RECALL": 100, "PRECISION": 35.29},
        "SASEXT":   {"#MATCHES": 26, "FP": 12, "FN": 0, "TP": 14, "RECALL": 100, "PRECISION": 53.85},
        "LIMECEP":  {"#MATCHES": 8, "FP": 0,  "FN": 0, "TP": 8, "RECALL": 100, "PRECISION": 100.0},
        "FLINKCEP": {"#MATCHES": 34, "FP": 22, "FN": 0, "TP": 12, "RECALL": 100, "PRECISION": 35.29}
    },
    "A+B+C": {
        "SASE":     {"#MATCHES": 284, "FP": 254, "FN": 0, "TP": 30, "RECALL": 100, "PRECISION": 10.56},
        "FLINKCEP": {"#MATCHES": 284, "FP": 254, "FN": 0, "TP": 30, "RECALL": 100, "PRECISION": 10.56},
        "LIMECEP":  {"#MATCHES": 6, "FP": 0, "FN": 0, "TP": 6, "RECALL": 100, "PRECISION": 100.0},
        "SASEXT":   {"#MATCHES": 6,   "FP": 0,   "FN": 0, "TP": 6, "RECALL": 100, "PRECISION": 100.0}
    }
}

# Use the desired order
patterns = ["ABC", "AB+C", "A+B+C"]
cep_systems = ["SASE", "SASEXT", "FLINKCEP", "LIMECEP"]


# Base color assignment (ensure LIMECEP is green)
base_colors = {
    "SASE": sns.color_palette("deep")[1],     # blue
    "SASEXT": sns.color_palette("deep")[0],   # orange
    "FLINKCEP": sns.color_palette("deep")[3], # red
    "LIMECEP": sns.color_palette("deep")[2] # green
}

# Function to darken or lighten a color
def adjust_color(color, amount=0.5):
    try:
        c = mcolors.to_rgb(color)
    except:
        c = mcolors.to_rgb("gray")
    return tuple(np.clip([1 - (1 - x) * amount for x in c], 0, 1))

# Colors for recall (lighter) and precision (darker)
recall_colors = {sys: adjust_color(base_colors[sys], amount=1.3) for sys in cep_systems}
precision_colors = {sys: adjust_color(base_colors[sys], amount=0.7) for sys in cep_systems}


# Plot parameters
bar_width = 0.08
n_patterns = len(patterns)
n_systems = len(cep_systems)
x = np.arange(n_patterns)  # pattern positions

# Create figure
plt.figure(figsize=(14, 6))

# Plot bars
for i, pattern in enumerate(patterns):
    for j, system in enumerate(cep_systems):
        entry = grouped_data.get(pattern, {}).get(system)
        if not entry:
            continue
        base_x = x[i] + (j - n_systems / 2) * (bar_width * 2.5)

        # Recall bar
        recall_height = entry["RECALL"]
        plt.bar(base_x, recall_height, width=bar_width, color=recall_colors[system], hatch='', label=f"{system} Recall" if i == 0 else "")
        plt.text(base_x, recall_height + 1, f"{recall_height:.0f}", ha='center', va='bottom', fontsize=14)

        # Precision bar
        precision_height = entry["PRECISION"]
        px = base_x + bar_width + 0.005
        plt.bar(px, precision_height, width=bar_width, color=precision_colors[system], hatch='//', label=f"{system} Precision" if i == 0 else "")
        plt.text(px, precision_height + 1, f"{precision_height:.0f}", ha='center', va='bottom', fontsize=14)

# Aesthetics
plt.xticks(x, patterns, fontsize=14)
plt.ylabel("Percentage (%)", fontsize=16)
# plt.title("Recall and Precision on duplicate occurrence (STNM)")
plt.grid(axis='y', linestyle='--', alpha=0.6)

# Deduplicated legend
handles, labels = plt.gca().get_legend_handles_labels()
unique = dict(zip(labels, handles))
plt.legend(unique.values(), unique.keys(), title="Metric", title_fontsize=16, fontsize=14, bbox_to_anchor=(1.02, 1), loc='upper left')

plt.tight_layout()
plt.savefig("exp3-stnm.png")