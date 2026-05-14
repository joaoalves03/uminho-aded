import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

plt.rcParams["svg.fonttype"] = "none"

sns.set_theme(
    style="whitegrid",
    rc={
        "font.family": "Linux Libertine",
        "axes.labelsize": 11,
        "xtick.labelsize": 10,
        "ytick.labelsize": 9,
    },
)

df = pd.read_csv("full.csv")


def clean_model(m):
    if "Llama" in m:
        return "LLama3.1"
    if "DeepSeek" in m or "Qwen3" in m:
        return "DeepseekV3.2"
    if "granite" in m.lower():
        return "Granite4.1"
    return m


df["model_clean"] = df["model"].apply(clean_model)

df["prompt_label"] = df["prompt_length"].map(
    {
        "short": "Short",
        "medium": "Medium",
        "long": "Long",
    }
)

df["group"] = df["model_clean"] + " - " + df["prompt_label"]

order = [
    f"{m} - {p}"
    for m in ["LLama3.1", "DeepseekV3.2", "Granite4.1"]
    for p in ["Short", "Medium", "Long"]
]

engine_colors = {
    "vanilla": "#4c78a8",
    "turboquant": "#e45756",
}

fig, (ax_ttft, ax_tpot) = plt.subplots(
    1,
    2,
    figsize=(6, 2.5),
    dpi=150,
    sharey=True,
)

sns.barplot(
    data=df,
    y="group",
    x="ttft_ms",
    hue="engine",
    hue_order=["vanilla", "turboquant"],
    palette=engine_colors,
    order=order,
    orient="h",
    errorbar=None,
    width=0.72,
    linewidth=0.4,
    edgecolor="black",
    ax=ax_ttft,
)

sns.barplot(
    data=df,
    y="group",
    x="predicted_per_token_ms",
    hue="engine",
    hue_order=["vanilla", "turboquant"],
    palette=engine_colors,
    order=order,
    orient="h",
    errorbar=None,
    width=0.72,
    linewidth=0.4,
    edgecolor="black",
    ax=ax_tpot,
)

for ax in [ax_ttft, ax_tpot]:
    ax.grid(
        axis="x",
        linestyle="--",
        linewidth=0.5,
        color="gray",
        alpha=0.6,
    )

    ax.grid(False, axis="y")
    ax.set_axisbelow(True)

    for spine in ax.spines.values():
        spine.set_linewidth(0.6)
        spine.set_color("black")

ax_ttft.set_xlabel("TTFT (ms)")
ax_ttft.set_ylabel("")

ax_tpot.set_xlabel("TPOT (ms / token)")
ax_tpot.set_ylabel("")

handles, labels = ax_tpot.get_legend_handles_labels()

ax_ttft.legend_.remove()
ax_tpot.legend_.remove()

fig.legend(
    handles,
    labels,
    loc="upper center",
    bbox_to_anchor=(0.5, 1.06),
    ncol=2,
    frameon=False,
    fontsize=8,
    handlelength=1.0,
    columnspacing=1.0,
)

ax_ttft.set_xlim(0, 12000)

tpot_max = df["predicted_per_token_ms"].max()
ax_tpot.set_xlim(0, tpot_max * 1.1)

plt.tight_layout(pad=0.6)

plt.savefig(
    "out/latency.svg",
    format="svg",
    bbox_inches="tight",
)

print("saved latency.svg")
