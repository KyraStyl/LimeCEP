import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import Patch

# --- Data ---
data = [
    ['ABC', 0.0, 'SASE', 8, 0, 0],
    ['ABC', 0.0, 'SASEXT', 15, 0, 0],
    ['ABC', 0.0, 'FLINKCEP', 15, 0, 0],
    ['ABC', 0.0, 'LIMECEP-NC', 15, 0, 0],
    ['ABC', 0.0, 'LIMECEP-C', 15, 0, 0],

    ['AB+C', 0.0, 'SASE', 12, 0, 0],
    ['AB+C', 0.0, 'SASEXT', 14, 0, 0],
    ['AB+C', 0.0, 'FLINKCEP', 28, 0, 0],
    ['AB+C', 0.0, 'LIMECEP-NC', 10, 0, 0],
    ['AB+C', 0.0, 'LIMECEP-C', 10, 0, 0],

    ['A+B+C', 0.0, 'SASE', 30, 0, 0],
    ['A+B+C', 0.0, 'SASEXT', 6, 0, 0],
    ['A+B+C', 0.0, 'FLINKCEP', 61, 0, 0],
    ['A+B+C', 0.0, 'LIMECEP-NC', 6, 0, 0],
    ['A+B+C', 0.0, 'LIMECEP-C', 6, 0, 0],

    ['ABC', 0.2, 'SASE', 1, 0, 7],
    ['ABC', 0.2, 'SASEXT', 8, 6, 1],
    ['ABC', 0.2, 'FLINKCEP', 15, 7, 13],
    ['ABC', 0.2, 'LIMECEP-NC', 15, 0, 0],
    ['ABC', 0.2, 'LIMECEP-C', 15, 0, 0],

    ['AB+C', 0.2, 'SASE', 1, 0, 11],
    ['AB+C', 0.2, 'SASEXT', 7, 7, 0],
    ['AB+C', 0.2, 'FLINKCEP', 12, 34, 16],
    ['AB+C', 0.2, 'LIMECEP-NC', 10, 0, 0],
    ['AB+C', 0.2, 'LIMECEP-C', 10, 0, 0],

    ['A+B+C', 0.2, 'SASE', 0, 25, 30],
    ['A+B+C', 0.2, 'SASEXT', 3, 4, 3],
    ['A+B+C', 0.2, 'FLINKCEP', 17, 94, 44],
    ['A+B+C', 0.2, 'LIMECEP-NC', 6, 0, 0],
    ['A+B+C', 0.2, 'LIMECEP-C', 6, 0, 0],

    ['ABC', 0.7, 'SASE', 1, 3, 7],
    ['ABC', 0.7, 'SASEXT', 6, 8, 9],
    ['ABC', 0.7, 'FLINKCEP', 5, 11, 10],
    ['ABC', 0.7, 'LIMECEP-NC', 13, 2, 0],
    ['ABC', 0.7, 'LIMECEP-C', 15, 0, 0],

    ['AB+C', 0.7, 'SASE', 1, 11, 11],
    ['AB+C', 0.7, 'SASEXT', 4, 5, 11],
    ['AB+C', 0.7, 'FLINKCEP', 10, 37, 18],
    ['AB+C', 0.7, 'LIMECEP-NC', 9, 1, 0],
    ['AB+C', 0.7, 'LIMECEP-C', 10, 0, 0],

    ['A+B+C', 0.7, 'SASE', 0, 14, 30],
    ['A+B+C', 0.7, 'SASEXT', 0, 2, 6],
    ['A+B+C', 0.7, 'FLINKCEP', 13, 164, 48],
    ['A+B+C', 0.7, 'LIMECEP-NC', 5, 1, 0],
    ['A+B+C', 0.7, 'LIMECEP-C', 6, 0, 0]

]

df = pd.DataFrame(data, columns=["PATTERN", "PROB", "CEP SYSTEM", "TP", "FP", "FN"])

df["TP"] = df["TP"].astype(int)
df["FP"] = df["FP"].astype(int)
df["FN"] = df["FN"].astype(int)

df_melt = df.melt(
    id_vars=["PATTERN", "PROB", "CEP SYSTEM"],
    value_vars=["TP", "FP", "FN"],
    var_name="Metric",
    value_name="Count"
)

# === Colors ===
base_colors = {
    "SASE": "#1f77b4",
    "SASEXT": "#ff7f0e",
    "FLINKCEP": "#d62728",#56B356
    "LIMECEP-NC": "#56B356",
    "LIMECEP-C":"#2ca02c"
}
shade_map = {"TP": 1.0, "FP": 0.6, "FN": 0.3}

def get_shaded_color(row):
    base = sns.color_palette([base_colors[row["CEP SYSTEM"]]])[0]
    factor = shade_map[row["Metric"]]
    return tuple(np.clip(np.array(base) * factor, 0, 1))

df_melt["Color"] = df_melt.apply(get_shaded_color, axis=1)

# === Structure ===
patterns = ["ABC", "AB+C", "A+B+C"]
probs = [0.0, 0.2, 0.7]
systems = list(base_colors.keys())
# yticksl = [10,20,30,40,50,60,70,80,90,100]

# === Plot setup ===
fig, ax = plt.subplots(figsize=(20, 8))
bar_width = 0.8
system_spacing = 1.0
x = 0

bar_positions = []
pattern_labels = []
prob_labels = []
pattern_ticks = []
prob_ticks = []
pattern_boundaries = []

# === Build the grouped bar chart ===
for pattern in patterns:
    pattern_start = x
    for prob in probs:
        prob_start = x
        for system in systems:
            group = df_melt[
                (df_melt["PATTERN"] == pattern) &
                (df_melt["PROB"] == prob) &
                (df_melt["CEP SYSTEM"] == system)
            ]
            bottom = 0
            for idx, row in group.iterrows():
                height = row["Count"]
                if height > 0:
                    ax.bar(x, height, bottom=bottom, color=row["Color"], width=bar_width)

                    # Label inside the bar
                    ax.text(
                        x,
                        bottom + height / 2,
                        str(height),
                        ha='center',
                        va='center',
                        fontsize=10,
                        color='white' if row["Metric"] != "FN" else 'red'
                    )

                    # Line between FP and FN
                    if row["Metric"] == "FP":
                        y_pos = bottom + height
                        ax.hlines(y=y_pos, xmin=x - bar_width / 2, xmax=x + bar_width / 2, color="black", linewidth=2)

                    bottom += height
            x += system_spacing
        # Subgroup label tick
        prob_center = (prob_start + x - system_spacing) / 2
        prob_ticks.append(prob_center)
        prob_labels.append(f"P={prob}")
        x += 1.0  # spacing between prob blocks
    # Pattern group label tick
    pattern_center = (pattern_start + x - 1.0 - system_spacing) / 2
    pattern_ticks.append(pattern_center)
    pattern_labels.append(pattern)
    pattern_boundaries.append(x - 0.5)  # line between groups
    x += 1.0  # spacing between pattern blocks

# === Main plot formatting ===
ax.set_xticks([])
ax.set_xlabel("")
ax.set_ylabel("Match Count", fontsize=20)
# ax.set_yticklabels(yticksl,fontsize=16)
# ax.set_title("Stacked TP / FP / FN per Pattern × Probability × System")
ax.set_xlim(-1, x + 1)  # Ensure full width including last group

# === Bottom axis: Probabilities
ax3 = ax.secondary_xaxis('bottom')
ax3.set_xticks(prob_ticks)
ax3.set_xticklabels(prob_labels, fontsize=20)
ax3.tick_params(pad=5)
# ax3.set_xlabel("Probability", labelpad=10)

# === Bottom axis: Patterns
ax4 = ax.secondary_xaxis('bottom')
ax4.set_xticks(pattern_ticks)
ax4.set_xticklabels(pattern_labels, fontsize=20, weight='bold')
ax4.tick_params(pad=25)
ax4.set_xlabel("Pattern", labelpad=10, fontsize=20)

# === Group dividers (corrected to end of each pattern block)
for bound in pattern_boundaries[:-1]:
    ax.axvline(bound + 0.5, linestyle="--", color="gray", alpha=0.4)

# === Legend inside the plot, top-left corner
legend_elements = []
for sys, base_col in base_colors.items():
    base = sns.color_palette([base_col])[0]
    for m in ["TP", "FP", "FN"]:
        color = tuple(np.clip(np.array(base) * shade_map[m], 0, 1))
        legend_elements.append(Patch(facecolor=color, label=f"{sys} - {m}"))

ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(0.01, 0.99), fontsize=20, frameon=False, ncol=3)

# === Final layout tweaks
# import matplotlib
# matplotlib.use('TkAgg')
plt.subplots_adjust(bottom=0.2, top=0.9, left=0.06, right=0.95)
# plt.show()
plt.savefig("plot1a-stam.png")
