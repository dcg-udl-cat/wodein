#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

ROOT = Path(__file__).resolve().parents[1]
BLOCKCHAIN_ROOT = ROOT / "benchmarks" / "blockchain"
BASELINE_ROOT = ROOT / "benchmarks" / "centralized_solution" / "benchmark-db"
OUTPUT_DIR = ROOT / "plots" / "results"

sns.set_theme(style="whitegrid", context="talk")


def dataset_fraction(name: str) -> int | None:
    match = re.search(r"_(\d{3})pct$", name)
    if match:
        return int(match.group(1))
    if name.endswith("_full") or name.endswith("_fullpct"):
        return 100
    return None


def dataset_label(pct: int) -> str:
    return f"{pct}%"


def compute_metrics(csv_path: Path, architecture: str, dataset_name: str, pct: int) -> dict:
    df = pd.read_csv(csv_path, low_memory=False)
    df["execution_duration_ms"] = pd.to_numeric(df["execution_duration_ms"], errors="coerce")

    order_tx = df[df["kind"] == "order_tx"].copy()
    lifecycle_tx = df[df["kind"] == "lifecycle_tx"].copy()
    order_batch = df[df["kind"] == "order_batch"].copy()

    order_tx_success = order_tx[order_tx["status"].str.lower() == "success"] if not order_tx.empty else order_tx

    lat = order_tx_success["execution_duration_ms"].dropna()

    p50 = float(lat.quantile(0.50)) if not lat.empty else np.nan
    p95 = float(lat.quantile(0.95)) if not lat.empty else np.nan
    p99 = float(lat.quantile(0.99)) if not lat.empty else np.nan
    mean = float(lat.mean()) if not lat.empty else np.nan

    lifecycle_total_s = float(lifecycle_tx["execution_duration_ms"].sum() / 1000.0)
    order_processing_total_s = float(order_batch["execution_duration_ms"].sum() / 1000.0)
    overall_runtime_s = lifecycle_total_s + order_processing_total_s

    success_orders = int(len(order_tx_success))
    total_orders = int(len(order_tx))
    success_rate_pct = float(success_orders / total_orders * 100.0) if total_orders else np.nan
    throughput_orders_s = float(success_orders / order_processing_total_s) if order_processing_total_s > 0 else np.nan

    return {
        "architecture": architecture,
        "dataset": dataset_name,
        "dataset_pct": pct,
        "dataset_label": dataset_label(pct),
        "csv_path": str(csv_path.relative_to(ROOT)),
        "orders_total": total_orders,
        "orders_success": success_orders,
        "success_rate_pct": success_rate_pct,
        "latency_p50_ms": p50,
        "latency_p95_ms": p95,
        "latency_p99_ms": p99,
        "latency_mean_ms": mean,
        "lifecycle_total_s": lifecycle_total_s,
        "order_processing_total_s": order_processing_total_s,
        "overall_runtime_s": overall_runtime_s,
        "throughput_orders_s": throughput_orders_s,
    }


def compute_session_metrics(csv_path: Path, architecture: str, dataset_name: str, pct: int) -> pd.DataFrame:
    df = pd.read_csv(csv_path, low_memory=False)
    df["execution_duration_ms"] = pd.to_numeric(df["execution_duration_ms"], errors="coerce")

    order_tx = df[df["kind"] == "order_tx"].copy()
    order_batch = df[df["kind"] == "order_batch"].copy()

    if order_tx.empty:
        return pd.DataFrame()

    order_tx = order_tx[order_tx["status"].str.lower() == "success"].copy()
    order_tx["auction_session"] = pd.to_numeric(order_tx["auction_session"], errors="coerce")
    order_batch["auction_session"] = pd.to_numeric(order_batch["auction_session"], errors="coerce")

    order_tx = order_tx.dropna(subset=["auction_session"]) 
    order_batch = order_batch.dropna(subset=["auction_session"]) 

    order_tx["auction_session"] = order_tx["auction_session"].astype(int)
    order_batch["auction_session"] = order_batch["auction_session"].astype(int)

    tx_group = (
        order_tx.groupby("auction_session")
        .agg(
            orders_success=("execution_duration_ms", "count"),
            mean_tx_ms=("execution_duration_ms", "mean"),
            p95_tx_ms=("execution_duration_ms", lambda x: x.quantile(0.95)),
        )
        .reset_index()
    )

    batch_group = (
        order_batch.groupby("auction_session", as_index=False)["execution_duration_ms"]
        .sum()
        .rename(columns={"execution_duration_ms": "session_runtime_ms"})
    )

    merged = tx_group.merge(batch_group, on="auction_session", how="outer").fillna(0)
    merged["session_runtime_s"] = merged["session_runtime_ms"] / 1000.0
    merged["throughput_orders_s"] = np.where(
        merged["session_runtime_s"] > 0,
        merged["orders_success"] / merged["session_runtime_s"],
        np.nan,
    )

    merged["architecture"] = architecture
    merged["dataset"] = dataset_name
    merged["dataset_pct"] = pct
    merged["dataset_label"] = dataset_label(pct)
    merged["csv_path"] = str(csv_path.relative_to(ROOT))

    cols = [
        "architecture",
        "dataset",
        "dataset_pct",
        "dataset_label",
        "auction_session",
        "orders_success",
        "mean_tx_ms",
        "p95_tx_ms",
        "session_runtime_s",
        "throughput_orders_s",
        "csv_path",
    ]
    return merged[cols].sort_values(["auction_session"]).reset_index(drop=True)


def load_architecture(root: Path, architecture: str) -> pd.DataFrame:
    rows: list[dict] = []
    for directory in sorted(root.iterdir()):
        if not directory.is_dir():
            continue
        pct = dataset_fraction(directory.name)
        if pct is None:
            continue
        csv_path = directory / "benchmark_results.csv"
        if not csv_path.exists():
            continue
        rows.append(compute_metrics(csv_path, architecture, directory.name, pct))

    result = pd.DataFrame(rows)
    if result.empty:
        raise RuntimeError(f"No benchmark CSV files found for {architecture} under {root}")

    return result.sort_values("dataset_pct").reset_index(drop=True)


def load_architecture_sessions(root: Path, architecture: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for directory in sorted(root.iterdir()):
        if not directory.is_dir():
            continue
        pct = dataset_fraction(directory.name)
        if pct is None:
            continue
        csv_path = directory / "benchmark_results.csv"
        if not csv_path.exists():
            continue
        frame = compute_session_metrics(csv_path, architecture, directory.name, pct)
        if not frame.empty:
            frames.append(frame)

    if not frames:
        raise RuntimeError(f"No session metrics found for {architecture} under {root}")

    return pd.concat(frames, ignore_index=True).sort_values(["dataset_pct", "auction_session"]).reset_index(drop=True)


def make_ratio_table(blockchain_df: pd.DataFrame, baseline_df: pd.DataFrame) -> pd.DataFrame:
    merged = blockchain_df.merge(
        baseline_df,
        on="dataset_pct",
        suffixes=("_blockchain", "_baseline"),
        how="inner",
    )

    ratio = pd.DataFrame(
        {
            "dataset_pct": merged["dataset_pct"],
            "dataset_label": merged["dataset_pct"].map(dataset_label),
            "orders": merged["orders_success_blockchain"],
            "latency_p50_factor_blockchain_vs_baseline": merged["latency_p50_ms_blockchain"]
            / merged["latency_p50_ms_baseline"],
            "latency_p95_factor_blockchain_vs_baseline": merged["latency_p95_ms_blockchain"]
            / merged["latency_p95_ms_baseline"],
            "runtime_factor_blockchain_vs_baseline": merged["overall_runtime_s_blockchain"]
            / merged["overall_runtime_s_baseline"],
            "throughput_factor_baseline_vs_blockchain": merged["throughput_orders_s_baseline"]
            / merged["throughput_orders_s_blockchain"],
        }
    )

    return ratio.sort_values("dataset_pct").reset_index(drop=True)


def save_tables(combined: pd.DataFrame, ratio: pd.DataFrame, sessions: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    combined.to_csv(OUTPUT_DIR / "benchmark_comparison_summary.csv", index=False)
    ratio.to_csv(OUTPUT_DIR / "benchmark_comparison_ratios.csv", index=False)
    sessions.to_csv(OUTPUT_DIR / "benchmark_session_breakdown.csv", index=False)

    session_totals = (
        sessions.pivot_table(
            index=["architecture", "dataset_pct", "dataset_label"],
            columns="auction_session",
            values="session_runtime_s",
            aggfunc="sum",
        )
        .reset_index()
        .rename(columns={1: "session_1_runtime_s", 2: "session_2_runtime_s", 3: "session_3_runtime_s"})
    )

    mean_tx = (
        sessions.groupby(["architecture", "dataset_pct"], as_index=False)["mean_tx_ms"]
        .mean()
        .rename(columns={"mean_tx_ms": "mean_tx_ms_across_sessions"})
    )
    total_runtime = (
        sessions.groupby(["architecture", "dataset_pct"], as_index=False)["session_runtime_s"]
        .sum()
        .rename(columns={"session_runtime_s": "total_session_runtime_s"})
    )

    session_totals = (
        session_totals.merge(mean_tx, on=["architecture", "dataset_pct"], how="left")
        .merge(total_runtime, on=["architecture", "dataset_pct"], how="left")
        .sort_values(["dataset_pct", "architecture"])
        .reset_index(drop=True)
    )
    session_totals.to_csv(OUTPUT_DIR / "benchmark_session_totals.csv", index=False)

    wide = (
        combined[
            [
                "architecture",
                "dataset_pct",
                "dataset_label",
                "orders_success",
                "latency_p50_ms",
                "latency_p95_ms",
                "latency_p99_ms",
                "overall_runtime_s",
                "throughput_orders_s",
            ]
        ]
        .sort_values(["dataset_pct", "architecture"])
        .copy()
    )

    def fmt(df: pd.DataFrame, decimals: int = 2) -> pd.DataFrame:
        out = df.copy()
        for col in out.columns:
            if pd.api.types.is_numeric_dtype(out[col]):
                out[col] = out[col].map(lambda v: f"{v:,.{decimals}f}" if pd.notna(v) else "")
        return out

    def to_markdown_table(df: pd.DataFrame) -> str:
        cols = list(df.columns)
        header = "| " + " | ".join(str(c) for c in cols) + " |"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |"
        rows = [
            "| " + " | ".join(str(df.iloc[i][c]) for c in cols) + " |"
            for i in range(len(df))
        ]
        return "\n".join([header, sep, *rows])

    summary_md = ["# Benchmark Comparison Summary", ""]
    summary_md.append("## Per-architecture metrics")
    summary_md.append(to_markdown_table(fmt(wide, 2)))
    summary_md.append("")
    summary_md.append("## Relative factors")
    summary_md.append(to_markdown_table(fmt(ratio, 2)))

    summary_md.append("")
    summary_md.append("## Session runtime totals (3 auction sessions)")
    summary_md.append(
        to_markdown_table(
            fmt(
                session_totals[
                    [
                        "architecture",
                        "dataset_pct",
                        "dataset_label",
                        "total_session_runtime_s",
                        "session_1_runtime_s",
                        "session_2_runtime_s",
                        "session_3_runtime_s",
                        "mean_tx_ms_across_sessions",
                    ]
                ],
                2,
            )
        )
    )

    med_latency_factor = ratio["latency_p95_factor_blockchain_vs_baseline"].median()
    med_runtime_factor = ratio["runtime_factor_blockchain_vs_baseline"].median()
    med_throughput_factor = ratio["throughput_factor_baseline_vs_blockchain"].median()

    summary_md.extend(
        [
            "",
            "## Key takeaways",
            f"- Median p95 latency factor (Blockchain vs Baseline): **{med_latency_factor:.1f}x**",
            f"- Median total replay runtime factor (Blockchain vs Baseline): **{med_runtime_factor:.1f}x**",
            f"- Median throughput factor (Baseline vs Blockchain): **{med_throughput_factor:.1f}x**",
        ]
    )

    (OUTPUT_DIR / "benchmark_comparison_report.md").write_text("\n".join(summary_md), encoding="utf-8")


def plot_metrics(combined: pd.DataFrame, ratio: pd.DataFrame, sessions: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 2, figsize=(16, 11), constrained_layout=True)

    ax = axes[0, 0]
    sns.lineplot(
        data=combined,
        x="dataset_pct",
        y="latency_p95_ms",
        hue="architecture",
        marker="o",
        linewidth=2.5,
        ax=ax,
    )
    ax.set_yscale("log")
    ax.set_title("Order p95 latency (log scale)")
    ax.set_xlabel("Dataset size (% of full day)")
    ax.set_ylabel("p95 latency (ms)")

    ax = axes[0, 1]
    sns.lineplot(
        data=combined,
        x="dataset_pct",
        y="throughput_orders_s",
        hue="architecture",
        marker="o",
        linewidth=2.5,
        ax=ax,
        legend=False,
    )
    ax.set_yscale("log")
    ax.set_title("Order throughput (log scale)")
    ax.set_xlabel("Dataset size (% of full day)")
    ax.set_ylabel("Throughput (orders/s)")

    ax = axes[1, 0]
    sns.barplot(
        data=combined,
        x="dataset_label",
        y="overall_runtime_s",
        hue="architecture",
        ax=ax,
    )
    ax.set_yscale("log")
    ax.set_title("Total replay runtime (log scale)")
    ax.set_xlabel("Dataset size")
    ax.set_ylabel("Runtime (s)")

    ax = axes[1, 1]
    ratio_long = ratio.melt(
        id_vars=["dataset_pct", "dataset_label"],
        value_vars=[
            "latency_p95_factor_blockchain_vs_baseline",
            "runtime_factor_blockchain_vs_baseline",
            "throughput_factor_baseline_vs_blockchain",
        ],
        var_name="metric",
        value_name="factor",
    )

    metric_labels = {
        "latency_p95_factor_blockchain_vs_baseline": "Latency p95 (Blockchain/Baseline)",
        "runtime_factor_blockchain_vs_baseline": "Runtime (Blockchain/Baseline)",
        "throughput_factor_baseline_vs_blockchain": "Throughput (Baseline/Blockchain)",
    }
    ratio_long["metric"] = ratio_long["metric"].map(metric_labels)

    sns.lineplot(
        data=ratio_long,
        x="dataset_pct",
        y="factor",
        hue="metric",
        marker="o",
        linewidth=2.2,
        ax=ax,
    )
    ax.set_yscale("log")
    ax.set_title("Relative performance factors")
    ax.set_xlabel("Dataset size (% of full day)")
    ax.set_ylabel("Factor (log scale)")

    out = OUTPUT_DIR / "benchmark_comparison_dashboard.png"
    fig.savefig(out, dpi=200)
    plt.close(fig)

    fig2, ax2 = plt.subplots(figsize=(12, 5.5), constrained_layout=True)
    sns.barplot(
        data=ratio.melt(
            id_vars=["dataset_label"],
            value_vars=[
                "latency_p50_factor_blockchain_vs_baseline",
                "latency_p95_factor_blockchain_vs_baseline",
                "runtime_factor_blockchain_vs_baseline",
                "throughput_factor_baseline_vs_blockchain",
            ],
            var_name="metric",
            value_name="factor",
        ),
        x="dataset_label",
        y="factor",
        hue="metric",
        ax=ax2,
    )
    ax2.set_yscale("log")
    ax2.set_xlabel("Dataset size")
    ax2.set_ylabel("Factor (log scale)")
    ax2.set_title("Relative factors by dataset")
    ax2.legend(title="Metric", fontsize=9)
    fig2.savefig(OUTPUT_DIR / "benchmark_comparison_factors.png", dpi=200)
    plt.close(fig2)

    full_sessions = sessions[sessions["dataset_pct"] == 100].copy()
    if not full_sessions.empty:
        fig3, axes3 = plt.subplots(1, 2, figsize=(14, 5.5), constrained_layout=True)

        ax3 = axes3[0]
        sns.barplot(
            data=full_sessions,
            x="auction_session",
            y="session_runtime_s",
            hue="architecture",
            ax=ax3,
        )
        ax3.set_yscale("log")
        ax3.set_title("Full dataset: runtime per auction session")
        ax3.set_xlabel("Auction session")
        ax3.set_ylabel("Session runtime (s, log scale)")

        ax4 = axes3[1]
        sns.barplot(
            data=full_sessions,
            x="auction_session",
            y="mean_tx_ms",
            hue="architecture",
            ax=ax4,
        )
        ax4.set_yscale("log")
        ax4.set_title("Full dataset: mean transaction time per session")
        ax4.set_xlabel("Auction session")
        ax4.set_ylabel("Mean transaction time (ms, log scale)")

        handles, labels = ax3.get_legend_handles_labels()
        ax3.legend_.remove()
        ax4.legend(handles, labels, title="Architecture")

        fig3.savefig(OUTPUT_DIR / "benchmark_sessions_full_dataset.png", dpi=200)
        plt.close(fig3)


def main() -> None:
    blockchain_df = load_architecture(BLOCKCHAIN_ROOT, "Blockchain")
    baseline_df = load_architecture(BASELINE_ROOT, "Baseline DB")
    blockchain_sessions = load_architecture_sessions(BLOCKCHAIN_ROOT, "Blockchain")
    baseline_sessions = load_architecture_sessions(BASELINE_ROOT, "Baseline DB")

    combined = pd.concat([blockchain_df, baseline_df], ignore_index=True).sort_values(
        ["dataset_pct", "architecture"]
    )
    sessions = pd.concat([blockchain_sessions, baseline_sessions], ignore_index=True).sort_values(
        ["dataset_pct", "auction_session", "architecture"]
    )
    ratio = make_ratio_table(blockchain_df, baseline_df)

    save_tables(combined, ratio, sessions)
    plot_metrics(combined, ratio, sessions)

    print(f"Saved outputs to {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
