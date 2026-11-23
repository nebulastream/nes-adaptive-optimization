#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib", "pandas"]
# ///
"""Plots the results of the ISQP scaling experiment (isqp-scaling-benchmark).

usage: plot.py <results.csv> [<results.csv> ...] [-o <output directory>]

Figures (one set per input file):
  batch-latency      request time over the number of change events per request
  batch-per-query    request time per affected query over the number of change events
  batch-breakdown    placement vs. deployment time (summed over the affected queries) of one thread
  sequence-latency   time of every request of a long sequence (rolling median), to show drift
"""

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402


def label(row) -> str:
    placement = "ISQP" if row["incremental"] else "holistic"
    mode = "OCC" if row["amendment_mode"] == "Optimistic" else "2PL"
    return f"{placement} {mode} t={row['threads']}"


def configurations(df: pd.DataFrame):
    keys = ["incremental", "amendment_mode", "threads"]
    for values, group in df.groupby(keys, sort=True):
        yield label(dict(zip(keys, values))), group


COLORS = {"ISQP OCC": "#1f77b4", "ISQP 2PL": "#2ca02c", "holistic OCC": "#ff7f0e", "holistic 2PL": "#d62728"}
LINESTYLES = ["-", "--", ":", "-."]
MARKERS = ["o", "s", "^", "D"]


def style(name: str, thread_counts: list) -> dict:
    """Color: placement and synchronization; line style and marker: number of threads."""
    family, threads = name.rsplit(" t=", 1)
    index = thread_counts.index(int(threads)) % len(LINESTYLES)
    return {"color": COLORS[family], "linestyle": LINESTYLES[index], "marker": MARKERS[index], "markersize": 4}


def title(df: pd.DataFrame) -> str:
    first = df.iloc[0]
    return (
        f"{first['mode']} mode: {first['cloud_nodes']} cloud / {first['fog_nodes']} fog / "
        f"{first['leaf_nodes']} leaf nodes, {first['queries']} {first.get('query', 'selection')} queries"
    )


def plot_batch(df: pd.DataFrame, out: Path, stem: str) -> None:
    batch = df[df["experiment"] == "batch"].copy()
    if batch.empty:
        return
    batch["processing_ms"] = batch["processing_us"] / 1000
    batch["per_query_ms"] = batch["processing_ms"] / batch["affected_queries"].clip(lower=1)

    for column, ylabel, name in [
        ("processing_ms", "request time [ms]", "batch-latency"),
        ("per_query_ms", "request time per affected query [ms]", "batch-per-query"),
    ]:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        thread_counts = sorted(batch["threads"].unique())
        for config, group in configurations(batch):
            stats = group.groupby("events")[column].agg(["median", "min", "max"])
            ax.errorbar(
                stats.index,
                stats["median"],
                yerr=[stats["median"] - stats["min"], stats["max"] - stats["median"]],
                label=config,
                capsize=2,
                **style(config, thread_counts),
            )
        ax.set_xscale("log", base=2)
        if column == "processing_ms":
            ax.set_yscale("log")
        ax.set_xlabel("topology change events per request")
        ax.set_ylabel(ylabel)
        ax.set_title(title(batch), fontsize=10)
        ax.grid(True, which="both", alpha=0.3)
        ax.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(out / f"{stem}-{name}.pdf")
        fig.savefig(out / f"{stem}-{name}.png", dpi=150)
        plt.close(fig)

    single = batch[batch["threads"] == batch["threads"].min()]
    groups = list(configurations(single))
    fig, axes = plt.subplots(1, len(groups), figsize=(3.2 * len(groups), 4), sharey=True, squeeze=False)
    for ax, (config, group) in zip(axes[0], groups):
        stats = group.groupby("events")[["placement_us_sum", "deployment_us_sum"]].median() / 1000
        positions = range(len(stats))
        ax.bar(positions, stats["placement_us_sum"], label="placement")
        ax.bar(positions, stats["deployment_us_sum"], bottom=stats["placement_us_sum"], label="deployment")
        ax.set_xticks(list(positions), [str(events) for events in stats.index], rotation=45)
        ax.set_title(config, fontsize=9)
        ax.set_xlabel("events per request")
    axes[0][0].set_ylabel("summed over affected queries [ms]")
    axes[0][0].legend(fontsize=8)
    fig.suptitle(title(batch), fontsize=10)
    fig.tight_layout()
    fig.savefig(out / f"{stem}-batch-breakdown.pdf")
    fig.savefig(out / f"{stem}-batch-breakdown.png", dpi=150)
    plt.close(fig)


def plot_sequence(df: pd.DataFrame, out: Path, stem: str) -> None:
    sequence = df[df["experiment"] == "sequence"].copy()
    if sequence.empty:
        return
    sequence["processing_ms"] = sequence["processing_us"] / 1000
    window = max(1, sequence["request"].max() // 25)
    fig, ax = plt.subplots(figsize=(7, 4.5))
    thread_counts = sorted(sequence["threads"].unique())
    for config, group in configurations(sequence):
        per_request = group.groupby("request")["processing_ms"].median()
        ax.plot(per_request.index, per_request.rolling(window, min_periods=1).median(), label=config, **style(config, thread_counts) | {"marker": None})
    ax.set_yscale("log")
    ax.set_xlabel("request number")
    ax.set_ylabel(f"request time [ms] (rolling median over {window})")
    moves = sequence["moves"].iloc[0]
    ax.set_title(f"{title(sequence)}, {2 * moves} events per request", fontsize=10)
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out / f"{stem}-sequence-latency.pdf")
    fig.savefig(out / f"{stem}-sequence-latency.png", dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results", nargs="+", type=Path)
    parser.add_argument("-o", "--output", type=Path, default=None, help="output directory (default: next to the CSV)")
    arguments = parser.parse_args()
    for path in arguments.results:
        df = pd.read_csv(path)
        out = arguments.output or path.parent
        out.mkdir(parents=True, exist_ok=True)
        plot_batch(df, out, path.stem)
        plot_sequence(df, out, path.stem)
        failed = df["failed_amendments"].sum()
        broken = df["broken_queries"].dropna().sum() if "broken_queries" in df else 0
        print(f"{path}: {len(df)} requests, {failed} failed amendments, {int(broken)} broken queries -> {out}")


if __name__ == "__main__":
    main()
