import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

df = pd.read_csv("full.csv")

df = df[["model", "engine", "prompt_length", "n", "ttft_ms"]]

df = df[df["prompt_length"] == "long"]

result = df.groupby(["model", "engine", "n"])["ttft_ms"].mean().reset_index()


def clean_model(m):
    if "Llama" in m:
        return "LLama3.1"
    if "DeepSeek" in m or "Qwen3" in m:
        return "DeepseekV3.2"
    if "granite" in m.lower():
        return "Granite4.1"
    return m


result["model_clean"] = result["model"].apply(clean_model)

result["engine_clean"] = result["engine"].str.capitalize()

pivot = result.pivot_table(
    index="n",
    columns=["engine_clean", "model_clean"],
    values="ttft_ms",
    aggfunc="mean",
)

pivot = pivot.round(0)

model_order = ["LLama3.1", "DeepseekV3.2", "Granite4.1"]
engine_order = ["Vanilla", "Turboquant"]

ordered_columns = [(engine, model) for model in model_order for engine in engine_order]
pivot = pivot.reindex(ordered_columns, axis=1)

new_labels = [f"{model}\n{engine}" for engine, model in pivot.columns]
pivot.columns = new_labels

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

fig, ax = plt.subplots(figsize=(7, 3.5), dpi=50)
sns.heatmap(
    pivot,
    annot=True,
    fmt=".0f",
    cmap=sns.color_palette("Blues", as_cmap=True),
    linewidths=0.5,
    linecolor="white",
    ax=ax,
)

ax.xaxis.tick_top()
ax.xaxis.set_label_position("top")
ax.set_xlabel("Model / Engine", labelpad=10)
ax.set_ylabel("Node Count")

plt.tight_layout()
plt.savefig("out/ttft_heatmap_long.svg", format="svg", bbox_inches="tight")
print("Saved ttft_heatmap_long.svg")
