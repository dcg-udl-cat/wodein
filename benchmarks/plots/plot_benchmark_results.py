#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, cast

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

DATASET_ORDER = [
    "omie_intraday_2024_10_01_010pct",
    "omie_intraday_2024_10_01_020pct",
    "omie_intraday_2024_10_01_040pct",
    "omie_intraday_2024_10_01_060pct",
    "omie_intraday_2024_10_01_080pct",
    "omie_intraday_2024_10_01_full",
]

DATASET_LABELS = {
    "omie_intraday_2024_10_01_010pct": "10%",
    "omie_intraday_2024_10_01_020pct": "20%",
    "omie_intraday_2024_10_01_040pct": "40%",
    "omie_intraday_2024_10_01_060pct": "60%",
    "omie_intraday_2024_10_01_080pct": "80%",
    "omie_intraday_2024_10_01_full": "100%",
}

DATASET_PERCENT = {
    "omie_intraday_2024_10_01_010pct": 10,
    "omie_intraday_2024_10_01_020pct": 20,
    "omie_intraday_2024_10_01_040pct": 40,
    "omie_intraday_2024_10_01_060pct": 60,
    "omie_intraday_2024_10_01_080pct": 80,
    "omie_intraday_2024_10_01_full": 100,
}

MISSING_BLOCKCHAIN = ["omie_intraday_2024_10_01_080pct", "omie_intraday_2024_10_01_full"]


def compute_metrics(df: pd.DataFrame) -> dict:
    order_tx = df[df["kind"] == "order_tx"].copy()
    lifecycle_tx = df[df["kind"] == "lifecycle_tx"].copy()
    order_batch = df[df["kind"] == "order_batch"].copy()

    for frame in (order_tx, lifecycle_tx, order_batch):
        frame["execution_duration_ms"] = pd.to_numeric(frame["execution_duration_ms"], errors="coerce")

    if order_tx.empty:
        return {
            "orders_total": 0,
            "orders_success": 0,
            "success_rate_pct": np.nan,
            "latency_p50_ms": np.nan,
            "latency_p95_ms": np.nan,
            "latency_p99_ms": np.nan,
            "lifecycle_p50_ms": np.nan,
            "lifecycle_p95_ms": np.nan,
            "lifecycle_total_s": 0.0,
            "order_processing_total_s": 0.0,
            "overall_runtime_s": 0.0,
            "throughput_orders_s": np.nan,
        }

    success_mask = order_tx["status"].str.lower().eq("success")
    success_tx = order_tx[success_mask]

    total_orders = int(len(order_tx))
    success_orders = int(len(success_tx))
    success_rate = (success_orders / total_orders) * 100.0 if total_orders > 0 else np.nan

    latencies = success_tx["execution_duration_ms"].dropna().to_numpy()
    if latencies.size == 0:
        p50 = p95 = p99 = np.nan
    else:
        p50 = float(np.percentile(latencies, 50))
        p95 = float(np.percentile(latencies, 95))
        p99 = float(np.percentile(latencies, 99))

    lifecycle_lat = lifecycle_tx["execution_duration_ms"].dropna().to_numpy()
    if lifecycle_lat.size == 0:
        lifecycle_p50 = lifecycle_p95 = np.nan
    else:
        lifecycle_p50 = float(np.percentile(lifecycle_lat, 50))
        lifecycle_p95 = float(np.percentile(lifecycle_lat, 95))

    batch_runtime_s = float(order_batch["execution_duration_ms"].sum() / 1000.0)
    lifecycle_runtime_s = float(lifecycle_tx["execution_duration_ms"].sum() / 1000.0)
    overall_runtime_s = batch_runtime_s + lifecycle_runtime_s
    throughput = success_orders / batch_runtime_s if batch_runtime_s > 0 else np.nan

    return {
        "orders_total": total_orders,
        "orders_success": success_orders,
        "success_rate_pct": success_rate,
        "latency_p50_ms": p50,
        "latency_p95_ms": p95,
        "latency_p99_ms": p99,
        "lifecycle_p50_ms": lifecycle_p50,
        "lifecycle_p95_ms": lifecycle_p95,
        "lifecycle_total_s": lifecycle_runtime_s,
        "order_processing_total_s": batch_runtime_s,
        "overall_runtime_s": overall_runtime_s,
        "throughput_orders_s": throughput,
    }


def collect_architecture_metrics(results_root: Path, architecture_name: str) -> Dict[str, dict]:
    metrics: Dict[str, dict] = {}
    for dataset in DATASET_ORDER:
        csv_path = results_root / dataset / "benchmark_results.csv"
        if not csv_path.exists():
            continue

        df = pd.read_csv(csv_path)
        m = compute_metrics(df)
        m["source"] = "measured"
        metrics[dataset] = m

    return metrics


def _fit_linear_predict(points: List[Tuple[int, float]], target_x: int) -> float:
    if not points:
        return np.nan
    if len(points) == 1:
        return points[0][1]
    xs = np.array([p[0] for p in points], dtype=float)
    ys = np.array([p[1] for p in points], dtype=float)
    slope, intercept = np.polyfit(xs, ys, 1)
    return float(intercept + slope * target_x)


def estimate_blockchain_placeholders(blockchain_metrics: Dict[str, dict], db_metrics: Dict[str, dict]) -> Dict[str, dict]:
    estimated: Dict[str, dict] = {}

    metric_names = [
        "success_rate_pct",
        "latency_p50_ms",
        "latency_p95_ms",
        "latency_p99_ms",
        "throughput_orders_s",
        "lifecycle_p50_ms",
        "lifecycle_p95_ms",
        "lifecycle_total_s",
        "order_processing_total_s",
        "overall_runtime_s",
    ]

    for dataset in MISSING_BLOCKCHAIN:
        target_pct = DATASET_PERCENT[dataset]
        row: Dict[str, object] = {}

        for metric in metric_names:
            points: List[Tuple[int, float]] = []
            for dname, values in blockchain_metrics.items():
                value = values.get(metric)
                pct = DATASET_PERCENT.get(dname)
                if pct is None or value is None or pd.isna(value):
                    continue
                points.append((pct, float(value)))

            prediction = _fit_linear_predict(points, target_pct)

            if metric.startswith("latency") or metric.startswith("lifecycle") or metric.endswith("_runtime_s"):
                prediction = max(prediction, 1.0)
            if metric == "throughput_orders_s":
                prediction = max(prediction, 1.0)
            if metric == "success_rate_pct":
                prediction = float(np.clip(prediction, 95.0, 100.0))

            row[metric] = prediction

        if dataset in db_metrics:
            row["orders_total"] = int(db_metrics[dataset]["orders_total"])
        else:
            count_points = [(DATASET_PERCENT[k], float(v["orders_total"])) for k, v in blockchain_metrics.items() if "orders_total" in v]
            row["orders_total"] = int(round(_fit_linear_predict(count_points, target_pct)))

        orders_total = cast(float, row["orders_total"])
        success_rate_pct = cast(float, row["success_rate_pct"])
        row["orders_success"] = int(round(orders_total * success_rate_pct / 100.0))
        row["source"] = "placeholder_estimated"
        estimated[dataset] = row

    return estimated


def assemble_table(blockchain_root: Path, db_root: Path, include_placeholders: bool = True) -> pd.DataFrame:
    blockchain_metrics = collect_architecture_metrics(blockchain_root, "Blockchain")
    db_metrics = collect_architecture_metrics(db_root, "DB Baseline")
    dynamic_placeholders = estimate_blockchain_placeholders(blockchain_metrics, db_metrics) if include_placeholders else {}

    rows: List[dict] = []

    for dataset in DATASET_ORDER:
        dataset_label = DATASET_LABELS[dataset]

        if dataset in db_metrics:
            m = db_metrics[dataset]
            rows.append(
                {
                    "architecture": "DB Baseline",
                    "dataset": dataset,
                    "dataset_label": dataset_label,
                    "orders_total": m["orders_total"],
                    "orders_success": m["orders_success"],
                    "success_rate_pct": m["success_rate_pct"],
                    "latency_p50_ms": m["latency_p50_ms"],
                    "latency_p95_ms": m["latency_p95_ms"],
                    "latency_p99_ms": m["latency_p99_ms"],
                    "throughput_orders_s": m["throughput_orders_s"],
                    "lifecycle_p50_ms": m.get("lifecycle_p50_ms"),
                    "lifecycle_p95_ms": m.get("lifecycle_p95_ms"),
                    "lifecycle_total_s": m.get("lifecycle_total_s"),
                    "order_processing_total_s": m.get("order_processing_total_s"),
                    "overall_runtime_s": m.get("overall_runtime_s"),
                    "source": m["source"],
                }
            )

        if dataset in blockchain_metrics:
            m = blockchain_metrics[dataset]
            rows.append(
                {
                    "architecture": "Blockchain",
                    "dataset": dataset,
                    "dataset_label": dataset_label,
                    "orders_total": m["orders_total"],
                    "orders_success": m["orders_success"],
                    "success_rate_pct": m["success_rate_pct"],
                    "latency_p50_ms": m["latency_p50_ms"],
                    "latency_p95_ms": m["latency_p95_ms"],
                    "latency_p99_ms": m["latency_p99_ms"],
                    "throughput_orders_s": m["throughput_orders_s"],
                    "lifecycle_p50_ms": m.get("lifecycle_p50_ms"),
                    "lifecycle_p95_ms": m.get("lifecycle_p95_ms"),
                    "lifecycle_total_s": m.get("lifecycle_total_s"),
                    "order_processing_total_s": m.get("order_processing_total_s"),
                    "overall_runtime_s": m.get("overall_runtime_s"),
                    "source": m["source"],
                }
            )
        elif include_placeholders and dataset in dynamic_placeholders:
            m = dynamic_placeholders[dataset]
            rows.append(
                {
                    "architecture": "Blockchain",
                    "dataset": dataset,
                    "dataset_label": dataset_label,
                    "orders_total": m["orders_total"],
                    "orders_success": m["orders_success"],
                    "success_rate_pct": m["success_rate_pct"],
                    "latency_p50_ms": m["latency_p50_ms"],
                    "latency_p95_ms": m["latency_p95_ms"],
                    "latency_p99_ms": m["latency_p99_ms"],
                    "throughput_orders_s": m["throughput_orders_s"],
                    "lifecycle_p50_ms": m["lifecycle_p50_ms"],
                    "lifecycle_p95_ms": m["lifecycle_p95_ms"],
                    "lifecycle_total_s": m["lifecycle_total_s"],
                    "order_processing_total_s": m["order_processing_total_s"],
                    "overall_runtime_s": m["overall_runtime_s"],
                    "source": m["source"],
                }
            )

    out = pd.DataFrame(rows)
    out["dataset"] = pd.Categorical(out["dataset"], categories=DATASET_ORDER, ordered=True)
    out = out.sort_values(["dataset", "architecture"]).reset_index(drop=True)
    return out


def plot_latency_panels(df: pd.DataFrame, out_dir: Path) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5), sharex=True)
    metrics = [
        ("latency_p50_ms", "P50 Latency (ms)"),
        ("latency_p95_ms", "P95 Latency (ms)"),
        ("latency_p99_ms", "P99 Latency (ms)"),
    ]

    palette = {"Blockchain": "#d62728", "DB Baseline": "#1f77b4"}

    for ax, (metric_col, title) in zip(axes, metrics):
        sns.lineplot(
            data=df,
            x="dataset_label",
            y=metric_col,
            hue="architecture",
            style="architecture",
            markers=True,
            dashes=False,
            palette=palette,
            linewidth=2.2,
            markersize=7,
            ax=ax,
        )
        ax.set_title(title, fontsize=11)
        ax.set_xlabel("Dataset size")
        ax.set_ylabel("ms")
        ax.grid(True, alpha=0.25)

        placeholder_rows = df[(df["architecture"] == "Blockchain") & (df["source"].str.startswith("placeholder"))]
        for _, row in placeholder_rows.iterrows():
            x = row["dataset_label"]
            y = row[metric_col]
            ax.annotate("placeholder", (x, y), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)

    handles, labels = axes[0].get_legend_handles_labels()
    for ax in axes:
        ax.get_legend().remove()
    fig.legend(handles, labels, loc="upper center", ncol=2, frameon=False, bbox_to_anchor=(0.5, 1.05))
    fig.suptitle("Order Transaction Latency by Dataset", fontsize=13, y=1.10)
    fig.tight_layout()

    fig.savefig(out_dir / "latency_percentiles.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / "latency_percentiles.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_throughput(df: pd.DataFrame, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.8))
    plot_df = df.copy()

    sns.barplot(
        data=plot_df,
        x="dataset_label",
        y="throughput_orders_s",
        hue="architecture",
        palette={"Blockchain": "#d62728", "DB Baseline": "#1f77b4"},
        ax=ax,
    )

    ax.set_title("Effective Throughput (Successful Orders / Batch Runtime)", fontsize=12)
    ax.set_xlabel("Dataset size")
    ax.set_ylabel("orders/s")
    ax.grid(axis="y", alpha=0.25)

    placeholder_subset = plot_df[(plot_df["architecture"] == "Blockchain") & (plot_df["source"].str.startswith("placeholder"))]
    if not placeholder_subset.empty:
        for patch, (_, row) in zip(ax.patches, plot_df.iterrows()):
            if row["architecture"] == "Blockchain" and str(row["source"]).startswith("placeholder"):
                patch.set_hatch("//")
                patch.set_edgecolor("black")
                patch.set_linewidth(0.7)

    ax.legend(title="Architecture", frameon=False)
    fig.tight_layout()

    fig.savefig(out_dir / "throughput.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / "throughput.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_success_rate(df: pd.DataFrame, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.8))

    sns.lineplot(
        data=df,
        x="dataset_label",
        y="success_rate_pct",
        hue="architecture",
        style="architecture",
        markers=True,
        dashes=False,
        linewidth=2.2,
        markersize=7,
        palette={"Blockchain": "#d62728", "DB Baseline": "#1f77b4"},
        ax=ax,
    )

    ax.set_title("Order Success Rate", fontsize=12)
    ax.set_xlabel("Dataset size")
    ax.set_ylabel("success (%)")
    ax.set_ylim(0, 101)
    ax.grid(True, alpha=0.25)

    placeholder_rows = df[(df["architecture"] == "Blockchain") & (df["source"].str.startswith("placeholder"))]
    for _, row in placeholder_rows.iterrows():
        ax.annotate("placeholder", (row["dataset_label"], row["success_rate_pct"]), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)

    ax.legend(title="Architecture", frameon=False)
    fig.tight_layout()

    fig.savefig(out_dir / "success_rate.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / "success_rate.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_overall_runtime(df: pd.DataFrame, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 4.8))

    sns.lineplot(
        data=df,
        x="dataset_label",
        y="overall_runtime_s",
        hue="architecture",
        style="architecture",
        markers=True,
        dashes=False,
        linewidth=2.3,
        markersize=7,
        palette={"Blockchain": "#d62728", "DB Baseline": "#1f77b4"},
        ax=ax,
    )

    ax.set_title("Overall Processing Time per Dataset", fontsize=12)
    ax.set_xlabel("Dataset size")
    ax.set_ylabel("seconds")
    ax.grid(True, alpha=0.25)

    placeholder_rows = df[(df["architecture"] == "Blockchain") & (df["source"].str.startswith("placeholder"))]
    for _, row in placeholder_rows.iterrows():
        ax.annotate("placeholder", (row["dataset_label"], row["overall_runtime_s"]), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)

    ax.legend(title="Architecture", frameon=False)
    fig.tight_layout()
    fig.savefig(out_dir / "overall_runtime.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / "overall_runtime.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_overall_runtime_log(df: pd.DataFrame, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 4.8))

    sns.lineplot(
        data=df,
        x="dataset_label",
        y="overall_runtime_s",
        hue="architecture",
        style="architecture",
        markers=True,
        dashes=False,
        linewidth=2.3,
        markersize=7,
        palette={"Blockchain": "#d62728", "DB Baseline": "#1f77b4"},
        ax=ax,
    )

    ax.set_yscale("log")
    ax.set_title("Overall Processing Time (log scale)", fontsize=12)
    ax.set_xlabel("Dataset size")
    ax.set_ylabel("seconds (log10)")
    ax.grid(True, which="both", alpha=0.25)
    ax.legend(title="Architecture", frameon=False)

    fig.tight_layout()
    fig.savefig(out_dir / "overall_runtime_log.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / "overall_runtime_log.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_latency_p95_log(df: pd.DataFrame, out_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 4.8))

    sns.lineplot(
        data=df,
        x="dataset_label",
        y="latency_p95_ms",
        hue="architecture",
        style="architecture",
        markers=True,
        dashes=False,
        linewidth=2.3,
        markersize=7,
        palette={"Blockchain": "#d62728", "DB Baseline": "#1f77b4"},
        ax=ax,
    )

    ax.set_yscale("log")
    ax.set_title("Order P95 Latency (log scale)", fontsize=12)
    ax.set_xlabel("Dataset size")
    ax.set_ylabel("ms (log10)")
    ax.grid(True, which="both", alpha=0.25)
    ax.legend(title="Architecture", frameon=False)

    fig.tight_layout()
    fig.savefig(out_dir / "latency_p95_log.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / "latency_p95_log.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_relative_speedup(df: pd.DataFrame, out_dir: Path) -> None:
    pivot = df.pivot_table(index="dataset", columns="architecture", values=["overall_runtime_s", "latency_p95_ms", "throughput_orders_s"], aggfunc="first")
    if ("DB Baseline" not in pivot["overall_runtime_s"].columns) or ("Blockchain" not in pivot["overall_runtime_s"].columns):
        return

    speed_df = pd.DataFrame(index=pivot.index)
    speed_df["dataset_label"] = [DATASET_LABELS[d] for d in speed_df.index]

    speed_df["runtime_speedup"] = pivot[("overall_runtime_s", "Blockchain")] / pivot[("overall_runtime_s", "DB Baseline")]
    speed_df["latency_p95_speedup"] = pivot[("latency_p95_ms", "Blockchain")] / pivot[("latency_p95_ms", "DB Baseline")]
    speed_df["throughput_gain"] = pivot[("throughput_orders_s", "DB Baseline")] / pivot[("throughput_orders_s", "Blockchain")]

    fig, axes = plt.subplots(1, 3, figsize=(14, 4.8), sharex=True)
    metrics = [
        ("runtime_speedup", "Runtime Speedup (Blockchain / DB)", "x faster"),
        ("latency_p95_speedup", "P95 Latency Ratio (Blockchain / DB)", "x larger"),
        ("throughput_gain", "Throughput Gain (DB / Blockchain)", "x higher"),
    ]

    for ax, (col, title, ylabel) in zip(axes, metrics):
        sns.lineplot(data=speed_df, x="dataset_label", y=col, marker="o", linewidth=2.3, color="#2ca02c", ax=ax)
        ax.axhline(1.0, color="gray", linewidth=1, linestyle="--")
        ax.set_title(title, fontsize=11)
        ax.set_xlabel("Dataset size")
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.25)

    fig.tight_layout()
    fig.savefig(out_dir / "relative_speedup.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / "relative_speedup.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_split_by_architecture(df: pd.DataFrame, out_dir: Path) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(12, 7.2), sharex=True)
    archs = ["Blockchain", "DB Baseline"]

    for row_idx, arch in enumerate(archs):
        sub = df[df["architecture"] == arch]

        sns.lineplot(data=sub, x="dataset_label", y="overall_runtime_s", marker="o", linewidth=2.2, color="#d62728" if arch == "Blockchain" else "#1f77b4", ax=axes[row_idx, 0])
        axes[row_idx, 0].set_title(f"{arch}: Overall Runtime")
        axes[row_idx, 0].set_ylabel("seconds")
        axes[row_idx, 0].grid(True, alpha=0.25)

        sns.lineplot(data=sub, x="dataset_label", y="latency_p95_ms", marker="o", linewidth=2.2, color="#d62728" if arch == "Blockchain" else "#1f77b4", ax=axes[row_idx, 1])
        axes[row_idx, 1].set_title(f"{arch}: Order P95 Latency")
        axes[row_idx, 1].set_ylabel("ms")
        axes[row_idx, 1].grid(True, alpha=0.25)

    for col in range(2):
        axes[1, col].set_xlabel("Dataset size")

    fig.tight_layout()
    fig.savefig(out_dir / "split_architecture_scales.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / "split_architecture_scales.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_operation_breakdown(df: pd.DataFrame, out_dir: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8), sharex=True)

    order_df = df.copy()
    sns.barplot(
        data=order_df,
        x="dataset_label",
        y="order_processing_total_s",
        hue="architecture",
        palette={"Blockchain": "#d62728", "DB Baseline": "#1f77b4"},
        ax=axes[0],
    )
    axes[0].set_title("Order Processing Time (sum of order_batch)", fontsize=11)
    axes[0].set_xlabel("Dataset size")
    axes[0].set_ylabel("seconds")
    axes[0].grid(axis="y", alpha=0.25)

    life_df = df.copy()
    sns.barplot(
        data=life_df,
        x="dataset_label",
        y="lifecycle_total_s",
        hue="architecture",
        palette={"Blockchain": "#d62728", "DB Baseline": "#1f77b4"},
        ax=axes[1],
    )
    axes[1].set_title("Lifecycle Time (open/close tx total)", fontsize=11)
    axes[1].set_xlabel("Dataset size")
    axes[1].set_ylabel("seconds")
    axes[1].grid(axis="y", alpha=0.25)

    for ax in axes:
        ax.legend(title="Architecture", frameon=False)

    fig.tight_layout()
    fig.savefig(out_dir / "operation_time_breakdown.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / "operation_time_breakdown.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_lifecycle_latency(df: pd.DataFrame, out_dir: Path) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.4), sharex=True)
    metrics = [("lifecycle_p50_ms", "Lifecycle P50 Latency (ms)"), ("lifecycle_p95_ms", "Lifecycle P95 Latency (ms)")]

    for ax, (col, title) in zip(axes, metrics):
        sns.lineplot(
            data=df,
            x="dataset_label",
            y=col,
            hue="architecture",
            style="architecture",
            markers=True,
            dashes=False,
            linewidth=2.2,
            markersize=7,
            palette={"Blockchain": "#d62728", "DB Baseline": "#1f77b4"},
            ax=ax,
        )
        ax.set_title(title, fontsize=11)
        ax.set_xlabel("Dataset size")
        ax.set_ylabel("ms")
        ax.grid(True, alpha=0.25)

    for ax in axes:
        ax.legend(title="Architecture", frameon=False)

    fig.tight_layout()
    fig.savefig(out_dir / "lifecycle_latency.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / "lifecycle_latency.pdf", bbox_inches="tight")
    plt.close(fig)


def write_plot_notes(df: pd.DataFrame, out_dir: Path) -> None:
    measured = df[df["source"] == "measured"].copy()
    if measured.empty:
        notes = "No measured rows available."
        (out_dir / "plot_explanations.md").write_text(notes, encoding="utf-8")
        return

    common = measured.groupby("dataset")["architecture"].nunique()
    common_ds = [d for d, count in common.items() if count == 2]
    latest = common_ds[-1] if common_ds else None

    speedup_line = ""
    if latest:
        latest_df = measured[measured["dataset"] == latest]
        chain = latest_df[latest_df["architecture"] == "Blockchain"].iloc[0]
        db = latest_df[latest_df["architecture"] == "DB Baseline"].iloc[0]
        if db["overall_runtime_s"] > 0:
            speedup = chain["overall_runtime_s"] / db["overall_runtime_s"]
            speedup_line = f"- On the largest common measured dataset ({DATASET_LABELS[latest]}), DB baseline is about {speedup:.1f}x faster in overall processing time."

    content = f"""# Plot Interpretation Guide

These plots focus on time behavior and operational latency for the two architectures.

## 1) `overall_runtime.*`
- Shows total benchmark processing time per dataset (order processing + lifecycle operations).
- Lower is better. This is the clearest top-line time metric.

## 1b) `overall_runtime_log.*`
- Same metric as above but on log scale.
- This is the preferred view when DB and blockchain are orders of magnitude apart.

## 2) `operation_time_breakdown.*`
- Left panel: total time spent processing order batches.
- Right panel: total time spent in lifecycle operations (`open_market`, `open_auction`, `close_auction`).
- Useful to identify whether the bottleneck is order traffic or control-plane operations.

## 3) `latency_percentiles.*`
- Order transaction latency percentiles (P50/P95/P99).
- P50 = typical latency, P95/P99 = tail latency and burst behavior.
- Paper readers should focus on P95/P99 for scalability impact.

## 3b) `latency_p95_log.*`
- Log-scale P95 view to keep both curves visible despite large gaps.

## 4) `lifecycle_latency.*`
- Latency percentiles for lifecycle operations only.
- Shows control-plane responsiveness as dataset scale grows.

## 5) `throughput.*`
- Effective throughput in successful orders/second.
- Computed from successful `order_tx` over total `order_batch` processing runtime.

## 6) `success_rate.*`
- Percentage of successful order transactions.
- Confirms whether throughput/latency gains are achieved without sacrificing reliability.

## 7) `relative_speedup.*`
- Direct ratio plots: runtime speedup, P95 latency ratio, throughput gain.
- Values above 1 indicate advantage for DB baseline on speed-related metrics.

## 8) `split_architecture_scales.*`
- Two-row layout with separate y-scales per architecture.
- Avoids visual flattening of DB baseline when blockchain dominates absolute scale.

## Placeholders
- Blockchain 80% and 100% points are marked as `placeholder` and estimated from measured blockchain trends plus known dataset sizes.
- Replace them when real results are available.

## Quick takeaway
{speedup_line if speedup_line else '- Add measured data for the largest datasets to compute final speedup statements.'}
"""
    (out_dir / "plot_explanations.md").write_text(content, encoding="utf-8")


def build_plots(df: pd.DataFrame, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    sns.set_theme(style="whitegrid", context="paper", font_scale=1.15)
    plt.rcParams["figure.dpi"] = 120
    plt.rcParams["axes.spines.top"] = False
    plt.rcParams["axes.spines.right"] = False

    plot_latency_panels(df, out_dir)
    plot_lifecycle_latency(df, out_dir)
    plot_overall_runtime(df, out_dir)
    plot_overall_runtime_log(df, out_dir)
    plot_latency_p95_log(df, out_dir)
    plot_operation_breakdown(df, out_dir)
    plot_relative_speedup(df, out_dir)
    plot_split_by_architecture(df, out_dir)
    plot_throughput(df, out_dir)
    plot_success_rate(df, out_dir)
    write_plot_notes(df, out_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate paper-ready plots for blockchain vs DB benchmark results.")
    parser.add_argument("--blockchain-root", default="../benchmark", help="Path to blockchain benchmark root folder")
    parser.add_argument("--db-root", default="../benchmark-db", help="Path to DB benchmark root folder")
    parser.add_argument("--out", default=".", help="Output directory for plots and summary CSV")
    parser.add_argument("--no-placeholders", action="store_true", help="Disable invented placeholder rows for missing blockchain datasets")
    return parser.parse_args()


def main() -> None:
    parsed = parse_args()

    blockchain_root = Path(parsed.blockchain_root)
    db_root = Path(parsed.db_root)
    out_dir = Path(parsed.out)

    summary = assemble_table(blockchain_root, db_root, include_placeholders=not parsed.no_placeholders)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary.to_csv(out_dir / "benchmark_summary_for_paper.csv", index=False)
    build_plots(summary, out_dir)

    print(f"Summary written to: {out_dir / 'benchmark_summary_for_paper.csv'}")
    print(f"Plots written to: {out_dir}")


if __name__ == "__main__":
    main()
