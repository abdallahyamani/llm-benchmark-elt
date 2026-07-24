import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from deltalake import DeltaTable

ROOT_DIR = Path(__file__).resolve().parents[1]
GOLD_DIR = ROOT_DIR / "data" / "gold"
DOCS_DIR = ROOT_DIR / "docs"
README_PATH = ROOT_DIR / "README.md"

START_MARKER = "<!-- RESULTS_START -->"
END_MARKER = "<!-- RESULTS_END -->"


def load_leaderboard() -> pd.DataFrame:
    """Load model_leaderboard Delta table as pandas DataFrame."""
    table_path = str(GOLD_DIR / "model_leaderboard")
    dt = DeltaTable(table_path)
    return dt.to_pandas()


def generate_chart(df: pd.DataFrame) -> Path:
    """Generate a line chart showing top model scores over time."""
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    chart_path = DOCS_DIR / "leaderboard.png"

    snapshots = sorted(df["snapshot_date"].unique())

    # Get today's top 5 models
    latest = df[df["snapshot_date"] == snapshots[-1]]
    top_models = latest.nsmallest(5, "rank")["model_name"].tolist()

    # Filter to just those models across all snapshots
    top_df = df[df["model_name"].isin(top_models)]

    fig, ax = plt.subplots(figsize=(10, 5))

    for model in top_models:
        model_data = top_df[top_df["model_name"] == model].sort_values("snapshot_date")
        ax.plot(model_data["snapshot_date"], model_data["composite_score"],
                marker="o", label=model, linewidth=2)

    ax.set_xlabel("Snapshot Date")
    ax.set_ylabel("Composite Score")
    ax.set_title("Top 5 LLM Models — Composite Score Over Time")
    ax.legend(loc="lower right", fontsize=8)
    ax.set_ylim(0, 100)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(chart_path, dpi=150)
    plt.close()

    return chart_path


def update_readme(chart_path: Path) -> None:
    """Update README between markers with the chart embed and date."""
    readme_content = README_PATH.read_text(encoding="utf-8")

    if START_MARKER not in readme_content:
        return

    latest_date = sorted(load_leaderboard()["snapshot_date"].unique())[-1]

    new_section = (
        f"{START_MARKER}\n"
        f"![Leaderboard](docs/leaderboard.png)\n\n"
        f"*Last updated: {latest_date}*\n"
        f"{END_MARKER}"
    )

    start_idx = readme_content.index(START_MARKER)
    end_idx = readme_content.index(END_MARKER) + len(END_MARKER)

    updated = readme_content[:start_idx] + new_section + readme_content[end_idx:]
    README_PATH.write_text(updated, encoding="utf-8")


def main():
    df = load_leaderboard()
    chart_path = generate_chart(df)
    update_readme(chart_path)
    print(f"Chart saved to {chart_path}")


if __name__ == "__main__":
    main()
