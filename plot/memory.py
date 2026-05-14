import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

plt.rcParams["axes.labelsize"] = 11
plt.rcParams["xtick.labelsize"] = 10
plt.rcParams["ytick.labelsize"] = 10

sns.set_theme(
    style="whitegrid",
    rc={
        "font.family": "Linux Libertine",
        "svg.fonttype": "none",
    },
)
sns.set_context("paper")

df = pd.read_csv("full.csv")

df = df[df["model"] == "Meta-Llama-3.1-8B-Instruct-Q4_K_M"]
df = df[(df["n"] == 1) & (df["concurrency"] == 1)]

df["peak_memory_mb"] = pd.to_numeric(df["peak_memory_mb"], errors="coerce")

df = df.dropna(subset=["peak_memory_mb"])

df["prompt_length"] = pd.Categorical(
    df["prompt_length"],
    categories=["short", "medium", "long"],
    ordered=True,
)

fig, ax = plt.subplots(figsize=(3.1, 2.0), dpi=150)

sns.barplot(
    data=df,
    x="prompt_length",
    y="peak_memory_mb",
    hue="engine",
    palette={
        "vanilla": "#4c78a8",
        "turboquant": "#e45756",
    },
    errorbar=None,
    width=0.72,
    linewidth=0.4,
    edgecolor="black",
    ax=ax,
)

ax.grid(
    axis="y",
    linestyle="--",
    linewidth=0.5,
    color="gray",
    alpha=0.6,
)

ax.grid(False, axis="x")

ax.set_axisbelow(True)

for spine in ax.spines.values():
    spine.set_linewidth(0.6)
    spine.set_color("black")

ax.set_xlabel("Prompt length", labelpad=2)
ax.set_ylabel("Peak memory (MB)", labelpad=4)

ax.set_xticklabels(["Short", "Medium", "Long"])

ax.legend(
    title=None,
    fontsize=8,
    frameon=False,
    loc="upper center",
    bbox_to_anchor=(0.5, 1.18),
    ncol=2,
    handlelength=1.0,
    columnspacing=1.0,
)

for container in ax.containers:
    ax.bar_label(
        container,
        fmt="%.0f",
        fontsize=8,
        padding=2,
        label_type="edge",
    )

ymax = df["peak_memory_mb"].max()
ax.set_ylim(0, ymax * 1.15)
plt.tight_layout(pad=0.3)

plt.savefig(
    "out/memory_comparison.svg",
    format="svg",
    bbox_inches="tight",
)
