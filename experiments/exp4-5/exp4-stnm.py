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

lat = {
    'ABC': {
        10: {'SASE': 1593658, 'FLINKCEP': 996000000, 'SASEXT': 264506254, 'LIMECEP': 12000000},
        100: {'SASE': 2334010, 'FLINKCEP': 1027000000, 'SASEXT': 2592020596050, 'LIMECEP': 20000000},
        1000: {'SASE': 3387513, 'FLINKCEP': 1031000000, 'SASEXT': None, 'LIMECEP': 26000000}
    },
    'AB+C': {
        10: {'SASE': 15221344, 'FLINKCEP': 1058000000, 'SASEXT': 158552031, 'LIMECEP': 16000000},
        100: {'SASE': 39327044, 'FLINKCEP': 1613000000, 'SASEXT': 733634822875, 'LIMECEP': 20000000},
        1000: {'SASE': 11118222999, 'FLINKCEP': 506235000000, 'SASEXT': None, 'LIMECEP': 62000000}
    },
    'A+B+C': {
        10: {'SASE': 7705629, 'FLINKCEP': 1049000000, 'SASEXT': 162439986, 'LIMECEP': 15000000},
        100: {'SASE': 513859741, 'FLINKCEP': 148302000000, 'SASEXT': 4854740748, 'LIMECEP': 86000000},
        1000: {'SASE': None, 'FLINKCEP': None, 'SASEXT': 1080837573801, 'LIMECEP': 38064000000}
    }
}



def plot_metric_line(metric_dict, title, ylabel, logscale=False):
    rows = []
    for pattern in patterns:
        for window in windows:
            for system in systems:
                value = metric_dict.get(pattern, {}).get(window, {}).get(system, None)
                if value is not None:
                    value_sec = value / 1000000000
                    rows.append({
                        'Pattern': pattern,
                        'Window': window,
                        'System': system,
                        'Value': value_sec
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
                ax.text(last["Window"] * 1.01, last["Value"] * 1.01, f"{system}", fontsize=12)

    if logscale:
        ax.set_yscale("log")

    ax.grid(True)
    ax.set_title(title)
    ax.set_xlabel("Window Size", fontsize=18)
    ax.set_ylabel(ylabel, fontsize=18)

    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + box.height * 0.2,
                     box.width, box.height * 0.8])

    ax.set_xlim(right=ax.get_xlim()[1] * 1.05)

    legend = ax.legend(
        loc='upper center',
        bbox_to_anchor=(0.5, -0.1),
        ncol=3,
        fontsize=12,
        title="System (Pattern)",
        title_fontsize=14,
        fancybox=True,
        shadow=True
    )

    plt.tight_layout()
    # plt.show()
    fig.savefig(
        f"exp4-stnm.png",
        bbox_extra_artists=(legend,),
        bbox_inches='tight'
    )


# === Call the plots ===
plot_metric_line(lat, "", "Time (s)", logscale=True)
