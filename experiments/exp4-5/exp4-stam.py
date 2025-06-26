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
        10: {'SASE': 6731394, 'FLINKCEP': 309000000, 'SASEXT': 264506254, 'LIMECEP': 17000000},
        100: {'SASE': 189824799, 'FLINKCEP': 10857000000, 'SASEXT': 2592020596050, 'LIMECEP': 216000000},
        1000: {'SASE': 135407319074, 'FLINKCEP': None, 'SASEXT': None, 'LIMECEP': None}
    },
    'AB+C': {
        10: {'SASE': 12210719, 'FLINKCEP': 263000000, 'SASEXT': 158552031, 'LIMECEP': 14000000},
        100: {'SASE': None, 'FLINKCEP': 49925000000, 'SASEXT': 733634822875, 'LIMECEP': 125000000},
        1000: {'SASE': None, 'FLINKCEP': None, 'SASEXT': None, 'LIMECEP': 4875000000}
    },
    'A+B+C': {
        10: {'SASE': 12210719, 'FLINKCEP': 275000000, 'SASEXT': 162439986, 'LIMECEP': 13000000},
        100: {'SASE': None, 'FLINKCEP': 38606000000, 'SASEXT': 4854740748, 'LIMECEP': 95000000},
        1000: {'SASE': None, 'FLINKCEP': None, 'SASEXT': 1080837573801, 'LIMECEP': 9238000000}
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
        f"exp4-stam.png",
        bbox_extra_artists=(legend,),
        bbox_inches='tight'
    )


# === Call the plots ===
plot_metric_line(lat, "", "Time (s)", logscale=True)
