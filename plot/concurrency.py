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

df = pd.read_csv("concurrency.csv")


def clean_model(m):
    if "Llama" in m:
        return "LLama3.1"
    if "DeepSeek" in m or "Qwen3" in m:
        return "DeepseekV3.2"
    if "granite" in m.lower():
        return "Granite4.1"
    return m


df["model_clean"] = df["model"].apply(clean_model)

baseline = (
    df[df["concurrency"] == 1]
    .groupby(["model_clean", "engine"])["predicted_per_second"]
    .median()
    .rename("baseline_tps")
)

df = df.join(baseline, on=["model_clean", "engine"])

df["slowdown"] = df["baseline_tps"] / df["predicted_per_second"]

df = df[df["concurrency"] != 1]

heatmap_df = df.groupby(["model_clean", "concurrency", "engine"], as_index=False).agg(
    avg_slowdown=("slowdown", "mean")
)

vanilla = heatmap_df[heatmap_df["engine"] == "vanilla"].pivot(
    index="model_clean", columns="concurrency", values="avg_slowdown"
)

turbo = heatmap_df[heatmap_df["engine"] == "turboquant"].pivot(
    index="model_clean", columns="concurrency", values="avg_slowdown"
)

fig, axes = plt.subplots(1, 2, figsize=(9, 3), dpi=150)

sns.heatmap(
    vanilla,
    ax=axes[0],
    annot=True,
    fmt=".2f",
    cmap="Blues",
    linewidths=0.5,
    vmin=1,
    vmax=max(vanilla.max().max(), turbo.max().max()),
)

axes[0].set_title("vanilla")
axes[0].set_xlabel("Concurrency")
axes[0].set_ylabel("Model")

sns.heatmap(
    turbo,
    ax=axes[1],
    annot=True,
    fmt=".2f",
    cmap="Blues",
    linewidths=0.5,
    vmin=1,
    vmax=max(vanilla.max().max(), turbo.max().max()),
)

axes[1].set_title("turboquant")
axes[1].set_xlabel("Concurrency")
axes[1].set_ylabel("")

plt.tight_layout()

plt.savefig(
    "out/concurrency_heatmap.svg",
    format="svg",
    bbox_inches="tight",
)

print("saved concurrency_heatmap.svg")
