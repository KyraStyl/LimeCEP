import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

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

df = pd.DataFrame(data, columns=["PATTERN", "PROBABILITY", "CEP SYSTEM", "TP", "FP", "FN"])

df["TP"] = df["TP"].astype(int)
df["FP"] = df["FP"].astype(int)
df["FN"] = df["FN"].astype(int)

df_melt = df.melt(
    id_vars=["PATTERN", "PROBABILITY", "CEP SYSTEM"],
    value_vars=["TP", "FP", "FN"],
    var_name="Metric",
    value_name="Count"
)


# --- Aggregate and compute metrics ---
agg = df.groupby(["CEP SYSTEM", "PROBABILITY"]).sum().reset_index()
agg["Precision"] = agg["TP"] / (agg["TP"] + agg["FP"])
agg["Recall"] = agg["TP"] / (agg["TP"] + agg["FN"])

# Add a small offset to Recall when it equals Precision
epsilon = 0.001
agg["Recall"] = np.where(
    agg["Precision"] == agg["Recall"],
    agg["Recall"] - epsilon,
    agg["Recall"]
)


# --- Melt for Seaborn long-form ---
long_df = agg.melt(
    id_vars=["CEP SYSTEM", "PROBABILITY"],
    value_vars=["Precision", "Recall"],
    var_name="Metric",
    value_name="Score"
)

# --- Seaborn lineplot ---
# --- Seaborn lineplot ---
fig, ax = plt.subplots(figsize=(10, 6))

sns.lineplot(
    data=long_df,
    x="PROBABILITY", y="Score",
    hue="CEP SYSTEM",
    style="Metric",
    markers=True,
    dashes=True,
    ax=ax
)

# Labels and axis styling
ax.set_xlabel("Probability", fontsize=20)
ax.set_ylabel("Score", fontsize=20)
ax.set_xticks([0.0, 0.2, 0.7])
ax.set_ylim(0, 1.05)
ax.tick_params(axis='both', labelsize=16)
ax.grid(True)

# Adjust plot area to make space for legend
box = ax.get_position()
ax.set_position([box.x0, box.y0 + box.height * 0.2,
                 box.width, box.height * 0.8])

# Add legend below
legend = ax.legend(
    loc='upper center',
    bbox_to_anchor=(0.5, -0.15),
    ncol=4,
    fontsize=14,
    title="System / Metric",
    title_fontsize=16,
    frameon=False
)


# plt.title("Precision and Recall vs Probability")
plt.xlabel("Probability", fontsize=20)
plt.ylabel("Score", fontsize=20)
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.ylim(0, 1.05)
plt.grid(True)
plt.tight_layout()
fig.tight_layout()
fig.savefig("exp1a-recall-precision-stam.png")
