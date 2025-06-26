import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Manual list of systems and window sizes
systems = ['SASE', 'FLINKCEP', 'SASEXT', 'LIMECEP']
windows = [10, 100, 1000]
pattern_markers = {
    'ABC': 'o',  # circle
    'AB+C': 's',  # square
    'A+B+C': 'D',  # diamond
}
pattern_styles = {
    'ABC': 'solid',  # solid line
    'AB+C': 'dashed',  # dashed line
    'A+B+C': 'dotted'  # dotted line
}

base_colors = {
    "SASE": sns.color_palette("deep")[1],     # blue
    "SASEXT": sns.color_palette("deep")[0],   # orange
    "FLINKCEP": sns.color_palette("deep")[3], # red
    "LIMECEP": sns.color_palette("deep")[2] # green
}

# Patterns
patterns = ['ABC', 'AB+C', 'A+B+C']

# MATCHES per pattern per window
matches = {
    'ABC': {
        10: {'SASE': 2929, 'FLINKCEP': 2929, 'SASEXT': 16546, 'LIMECEP': 2929},
        100: {'SASE': 3232, 'FLINKCEP': 3232, 'SASEXT': 1814393, 'LIMECEP': 3232},
        1000: {'SASE': 3232, 'FLINKCEP': 3232, 'SASEXT': None, 'LIMECEP': 3232}
    },
    'AB+C': {
        10: {'SASE': 7764, 'FLINKCEP': 7764, 'SASEXT': 10474, 'LIMECEP': 2929},
        100: {'SASE': 105723, 'FLINKCEP': 105723, 'SASEXT': 911289, 'LIMECEP': 3232},
        1000: {'SASE': 1033717, 'FLINKCEP': 1033717, 'SASEXT': None, 'LIMECEP': 3232}
    },
    'A+B+C': {
        10: {'SASE': 8245, 'FLINKCEP': 16374, 'SASEXT': 4689, 'LIMECEP': 3268},
        100: {'SASE': 1559380, 'FLINKCEP': 1745254, 'SASEXT': 54713, 'LIMECEP': 32927},
        1000: {'SASE': None, 'FLINKCEP': None, 'SASEXT': 528592, 'LIMECEP': 295815}
    }
}

exec_time = {
    'ABC': {
        10: {'SASE': 0.1, 'FLINKCEP': 0.2, 'SASEXT': 0.4, 'LIMECEP': 5},
        100: {'SASE': 0.1, 'FLINKCEP': 10, 'SASEXT': 7997, 'LIMECEP': 7},
        1000: {'SASE': 0.1, 'FLINKCEP': 10, 'SASEXT': None, 'LIMECEP': 10}
    },
    'AB+C': {
        10: {'SASE': 0.2, 'FLINKCEP': 0.2, 'SASEXT': 0.2, 'LIMECEP': 7},
        100: {'SASE': 1.4, 'FLINKCEP': 10, 'SASEXT': 2349, 'LIMECEP': 12},
        1000: {'SASE': 95, 'FLINKCEP': 540, 'SASEXT': None, 'LIMECEP': 20}
    },
    'A+B+C': {
        10: {'SASE': 0.2, 'FLINKCEP': 8, 'SASEXT': 0.2, 'LIMECEP': 4},
        100: {'SASE': 20.56, 'FLINKCEP': 180, 'SASEXT': 5, 'LIMECEP': 4},
        1000: {'SASE': None, 'FLINKCEP': None, 'SASEXT': 1080, 'LIMECEP': 293}
    }
}

# MEMORY (MB)
memory = {
    'ABC': {
        10: {'SASE': 70, 'FLINKCEP': 900, 'SASEXT': 70, 'LIMECEP': 50},
        100: {'SASE': 100, 'FLINKCEP': 900, 'SASEXT': 660, 'LIMECEP': 50},
        1000: {'SASE': 100, 'FLINKCEP': 1000, 'SASEXT': 8000, 'LIMECEP': 50}
    },
    'AB+C': {
        10: {'SASE': 100, 'FLINKCEP': 1000, 'SASEXT': 150, 'LIMECEP': 50},
        100: {'SASE': 250, 'FLINKCEP': 1500, 'SASEXT': 800, 'LIMECEP': 170},
        1000: {'SASE': 1100, 'FLINKCEP': 3200, 'SASEXT': 8000, 'LIMECEP': 1200}
    },
    'A+B+C': {
        10: {'SASE': 100, 'FLINKCEP': 1200, 'SASEXT': 150, 'LIMECEP': 50},
        100: {'SASE': 900, 'FLINKCEP': 3000, 'SASEXT': 300, 'LIMECEP': 100},
        1000: {'SASE': 6500, 'FLINKCEP': 4000, 'SASEXT': 5000, 'LIMECEP': 1000}
    }
}

# CPU (%)
cpu = {
    'ABC': {
        10: {'SASE': 1, 'FLINKCEP': 15, 'SASEXT': 15, 'LIMECEP': 9},
        100: {'SASE': 1, 'FLINKCEP': 17, 'SASEXT': 15, 'LIMECEP': 12},
        1000: {'SASE': 1, 'FLINKCEP': 30, 'SASEXT': 15, 'LIMECEP': 15}
    },
    'AB+C': {
        10: {'SASE': 1, 'FLINKCEP': 17, 'SASEXT': 15, 'LIMECEP': 11},
        100: {'SASE': 1, 'FLINKCEP': 18, 'SASEXT': 15, 'LIMECEP': 12},
        1000: {'SASE': 6, 'FLINKCEP': 20, 'SASEXT': 15, 'LIMECEP': 12}
    },
    'A+B+C': {
        10: {'SASE': 1, 'FLINKCEP': 17, 'SASEXT': 15, 'LIMECEP': 10},
        100: {'SASE': 6, 'FLINKCEP': 20, 'SASEXT': 15, 'LIMECEP': 12},
        1000: {'SASE': 18, 'FLINKCEP': 20, 'SASEXT': 15, 'LIMECEP': 12}
    }
}

# === Enhanced line plot with annotations and styles ===
def plot_metric_line(metric_dict, title, ylabel, logscale=False):
    rows = []
    for pattern in patterns:
        for window in windows:
            for system in systems:
                value = metric_dict.get(pattern, {}).get(window, {}).get(system, None)
                if value is not None:
                    rows.append({
                        'Pattern': pattern,
                        'Window': window,
                        'System': system,
                        'Value': value
                    })

    df = pd.DataFrame(rows)

    plt.figure(figsize=(10, 6))

    for pattern in patterns:
        for system in systems:
            df_line = df[(df['Pattern'] == pattern) & (df['System'] == system)]
            if not df_line.empty:
                sns.lineplot(
                    data=df_line,
                    x="Window", y="Value",
                    label=f"{system} ({pattern})",
                    linestyle=pattern_styles.get(pattern, 'solid'),
                    marker=pattern_markers.get(pattern, 'o')
                )
                # Annotate last point
                last = df_line.sort_values("Window").iloc[-1]
                plt.text(last["Window"] * 1.01, last["Value"] * 1.01, f"{system}", fontsize=14)

    if logscale:
        plt.yscale("log")

    plt.grid(True)
    plt.title(title)
    plt.xlabel("Window Size", fontsize=16)
    plt.ylabel(ylabel)
    plt.legend(title="System (Pattern)", fontsize=9)
    plt.tight_layout()
    metric='exec-time'
    if not logscale:
        metric='cpu'
    plt.savefig("exp5-"+metric+"-stnm.png")

def plot_metric_line_new_plot(metric_dict, title, ylabel, logscale=False):
    rows = []
    for pattern in patterns:
        for window in windows:
            for system in systems:
                value = metric_dict.get(pattern, {}).get(window, {}).get(system, None)
                if value is not None:
                    rows.append({
                        'Pattern': pattern,
                        'Window': window,
                        'System': system,
                        'Value': value
                    })

    df = pd.DataFrame(rows)

    fig = plt.figure(figsize=(10, 6))
    ax = fig.add_subplot(111)

    for pattern in patterns:
        for system in systems:
            df_line = df[(df['Pattern'] == pattern) & (df['System'] == system)]
            if not df_line.empty:
                sns.lineplot(
                    ax=ax,
                    data=df_line,
                    x="Window", y="Value",
                    label=f"{system} ({pattern})",
                    linestyle=pattern_styles.get(pattern, 'solid'),
                    marker=pattern_markers.get(pattern, 'o')
                )
                last = df_line.sort_values("Window").iloc[-1]
                ax.text(last["Window"] * 1.01, last["Value"] * 1.01, f"{system}", fontsize=10)

    if logscale:
        ax.set_yscale("log")

    ax.grid(True)
    ax.set_title(title)
    ax.set_xlabel("Window Size", fontsize=18)
    ax.set_ylabel(ylabel, fontsize=18)

    ax.set_xlim(right=ax.get_xlim()[1] * 1.05)

    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + box.height * 0.2,
                     box.width, box.height * 0.8])

    legend = ax.legend(
        loc='upper center',
        bbox_to_anchor=(0.5, -0.1),
        ncol=3,
        fontsize=14,
        title="System (Pattern)",
        title_fontsize=16,
        fancybox=True,
        shadow=True
    )

    plt.tight_layout()
    metric='exec-time'
    if not logscale:
        metric='cpu'
    # plt.savefig("exp3-"+metric+"-stnm.png")
    # plt.show()
    fig.savefig(
        f"exp5-{metric}-stnm.png",
        bbox_extra_artists=(legend,),
        bbox_inches='tight'
    )

# === Enhanced memory barplot (log + labels) ===
def plot_memory_bar(metric_dict):
    rows = []
    for pattern in patterns:
        for window in windows:
            for system in systems:
                value = metric_dict.get(pattern, {}).get(window, {}).get(system, None)
                if value is not None:
                    rows.append({
                        'Pattern': pattern,
                        'Window': window,
                        'System': system,
                        'Memory': value
                    })

    df = pd.DataFrame(rows)

    # Create the catplot with correct palette and hue
    g = sns.catplot(
        data=df, kind="bar",
        x="Pattern", y="Memory",
        hue="System", col="Window",
        sharey=True, height=5, aspect=1,
        palette=base_colors
    )

    g.set_titles("Window: {col_name}", size=16)
    g.set_axis_labels("Pattern", "Memory (MB)", fontsize=18)
    max_y = 10 ** 4
    for ax in g.axes.flatten():
        ax.set_ylim(1, max_y)
    g.set(yscale="log")
    g.set_xticklabels(rotation=0)

    for ax in g.axes.flatten():
        ax.tick_params(axis='x', labelsize=14)
        ax.tick_params(axis='y', labelsize=14)

    # Annotate bars with values
    for ax in g.axes.flatten():
        ax.grid(True)
        for container in ax.containers:
            for bar in container:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2., height * 1.05,
                            f'{int(height)}', ha='center', va='bottom', fontsize=12)

    # g.fig.suptitle("Memory Usage per Pattern (Log Scale)", fontsize=14)
    plt.tight_layout(rect=[0, 0.05, 0.92, 0.95])  # Adjust for legend space
    # g.add_legend(title="System", loc='lower center', bbox_to_anchor=(0.5, -0.05))
    plt.savefig("exp5-mem-stnm.png")

# === Call the plots ===
plot_metric_line_new_plot(exec_time, "", "Time (s)", logscale=True)
plot_memory_bar(memory)
plot_metric_line_new_plot(cpu, "", "CPU (%)", logscale=False)

