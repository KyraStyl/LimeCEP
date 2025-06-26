import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


# Patterns and their memory in "single execution"
data = {
    "window_100": {
        "single": {"ABC": 50, "AB+C": 50, "A+B+C": 50, "BCA": 50, "C+A+B": 50},
        "double": {"ABC": 50, "AB+C": 50},
        "multiple": {
    "ABC": 20,
    "AB+C": 20,
    "A+B+C": 20,
    "BCA": 20,
    "C+A+B": 20
}    },
    "window_1000": {
        "single": {"ABC": 50, "AB+C": 1200, "A+B+C": 1200, "BCA": 50, "C+A+B": 600},
        "multiple": {"ABC": 51.61, "AB+C": 1238.71, "A+B+C": 1238.71, "BCA": 51.61, "C+A+B": 619.35 }
    }
}

patterns = ['ABC', 'AB+C', 'A+B+C', 'BCA', 'C+A+B']
colors = {
    'ABC': '#1f77b4',
    'AB+C': '#2ca02c',
    'A+B+C': '#d62728',
    'BCA': '#9467bd',
    'C+A+B': '#8c564b'
}

patterns = ['ABC', 'AB+C', 'A+B+C', 'BCA', 'C+A+B']
colors = dict(zip(patterns, sns.color_palette("Set2", len(patterns))))
hatches = ['///', '\\\\\\', 'xxx', '---', '+++']

fig, ax = plt.subplots(figsize=(16, 6))

# Layout params
bar_width = 0.3
bar_spacing = 0.4
group_spacing = 1.2

x = 0
group_centers = []

for window_name in ['window_100', 'window_1000']:
    config = data[window_name]

    # --- SINGLE ---
    group_start = x
    for i, pattern in enumerate(patterns):
        val = config['single'][pattern]
        ax.bar(x, val, width=bar_width, color=colors[pattern], hatch=hatches[i])
        x += bar_spacing
    center = (group_start + x - bar_spacing) / 2
    group_centers.append((center, f"Single\n{window_name.replace('_', '')}"))


    # --- DOUBLE ---
    if "double" in config:
        x += group_spacing
        double_x = x
        bottom = 0
        total_double = sum(config['double'].values())
        for i, pattern in enumerate(patterns):
            val = config['double'].get(pattern, 0)
            if val > 0:
                pct = val / total_double * 100
                ax.bar(x, pct, bottom=bottom, width=bar_width, color=colors[pattern], hatch=hatches[i])
                bottom += pct
        group_centers.append((double_x, f"Double\n{window_name.replace('_', '')}"))

    # --- MULTIPLE ---
    x += group_spacing
    multi_x = x
    bottom = 0
    for i, pattern in enumerate(patterns):
        val = config['multiple'].get(pattern, 0)
        ax.bar(x, val, bottom=bottom, width=bar_width, color=colors[pattern], hatch=hatches[i])
        bottom += val
    group_centers.append((multi_x, f"Multiple\n{window_name.replace('_', '')}"))

    x += group_spacing  # Move to next window group

# Set x-ticks to group centers only
group_x, group_labels = zip(*group_centers)
ax.set_xticks(group_x)
ax.set_xticklabels(group_labels, rotation=0, fontsize=14)


# Add separators between groups
for gx, _ in group_centers:
    ax.axvline(gx + bar_width / 2 + 0.15, color='lightgray', linestyle='--', linewidth=0.6)

# # Add value labels on bars
# for container in ax.containers:
#     ax.bar_label(container, fmt='%.0f', fontsize=8, padding=2)


# Add total value label on top of each bar group (Double/Multiple only)
plotted_x = set()

for container in ax.containers:
    for bar in container:
        xval = bar.get_x()
        height = bar.get_height()

        # To avoid duplicate labeling, only label once per bar group
        if xval in plotted_x:
            continue

        # Find all bars at this x position (stacked segments)
        total = sum(p.get_height() for c in ax.containers for p in c if p.get_x() == xval)
        if total > 0:
            ax.text(
                xval + bar.get_width() / 2,  # center of bar
                total + 10,                  # just above the bar
                f"{int(round(total))}",     # rounded total
                ha='center',
                va='bottom',
                fontsize=11,
                fontweight='bold'
            )
            plotted_x.add(xval)


# Axis labels and legend
ax.set_ylabel("Memory Usage in MB", fontsize=16)
ax.set_yticklabels(ax.get_yticklabels(), fontsize=14)
ax.set_xlabel(
    "Number of Patterns Under Detection",
    fontsize=14,
    labelpad=10
)
# ax.set_title("Memory Usage for different number of pattern under detection", fontsize=16)

legend_patches = [
    plt.Rectangle((0, 0), 1, 1, facecolor=colors[p], hatch=hatches[i], label=p)
    for i, p in enumerate(patterns)
]
ax.legend(handles=legend_patches, title="Pattern", title_fontsize=16, fontsize=14, bbox_to_anchor=(1.05, 1), loc='upper left')

ax.grid(axis='y')
plt.tight_layout()
plt.savefig("exp6.png")
# plt.show()