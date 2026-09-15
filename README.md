# M5 Spark Pipeline

PySpark pipeline over **59 million rows** of M5 retail sales: explicit-schema ingestion, wide→long reshape, partitioned Parquet, a join benchmark showing a **6.8× speedup** from forced sort-merge to broadcast + AQE, and windowed features — with physical-plan analysis at each step.

Built on Databricks serverless (Photon). Three notebooks, each one runnable top to bottom.

## Problem

Retail sales arrive *wide*: one row per item-store, one column per day (1,941 of them). Downstream analytics — price joins, rolling features, per-store queries — need the data *long*, partitioned, and enriched. At 59M rows that is past pandas territory, and the interesting engineering questions (where do shuffles happen, which joins should broadcast, what does an inner join silently throw away) only show up at this scale.

## Data

[M5 Forecasting – Accuracy](https://www.kaggle.com/competitions/m5-forecasting-accuracy) (Kaggle, Walmart)

| File | Shape | Role |
|---|---|---|
| `sales_train_evaluation.csv` | 30,490 × 1,947 | fact, wide |
| `calendar.csv` | 1,969 rows | date dimension (`d` → date, week, events, SNAP) |
| `sell_prices.csv` | 6,841,121 rows | price per (store, item, week) — **only for weeks an item was listed** |

After reshape: **59,181,090 rows → ~50 MB snappy Parquet**, 10 partitions by `store_id` (5,918,109 rows each, no skew).

## Pipeline

```
sales_train_evaluation.csv ──(01)── stack() ──▶ out/sales_long        59.2M × 8   partitioned by store_id
                                                     │
calendar.csv ────────────────(03)── LEFT join ───────┤
sell_prices.csv ─────────────(03)── LEFT join ───────┴─▶ out/sales_joined     59.2M × 16  + is_listed flag
                                                                 │
                                                (03)── window fns ┴─▶ out/sales_features  59.2M × 19  28d avg, lag-7, days_since_listed
```

`02_join_benchmark` sits beside the pipeline: same joins, three execution strategies, timed.

| Notebook | What it does |
|---|---|
| [`01_load_and_melt.py`](notebooks/01_load_and_melt.py) | Explicit-schema CSV read → `stack()` unpivot → Parquet partitioned by `store_id`; measures file layout with/without `repartition` |
| [`02_join_benchmark.py`](notebooks/02_join_benchmark.py) | `long ⋈ calendar ⋈ prices` three ways: forced sort-merge, manual broadcast, AQE-decided; reads the physical plans |
| [`03_features.py`](notebooks/03_features.py) | Canonical LEFT-joined table with `is_listed`; rolling 28-day average, 7-day lag, days since listing via window functions; benchmarks aligned vs. misaligned window specs |

## Benchmark

Databricks serverless, Photon enabled. Wall-clock including `count()`.

| Step | Time | Notes |
|---|---|---|
| Read 30,490 × 1,947 CSV, `inferSchema=True` | 3.5s | full extra pass to infer 1,947 column types |
| Same read, explicit schema | **1.2s** | 2.9× |
| `stack()` → 59.2M rows + count | 5.3s | narrow transformation, no shuffle |
| Write `partitionBy("store_id")` | 6.2s | 1–2 files per store |
| Write `repartition("store_id")` + `partitionBy` | 9.8s | exactly 1 file per store; extra shuffle |
| **Join A** — 2× sort-merge (forced via `hint("merge")`) | 32.7s | 4 shuffles, 2 sorts, `SortMergeJoin` falls out of Photon |
| **Join B** — `broadcast(cal)` + sort-merge `prices` | 10.0s | 2 shuffles; the 59M-row side never moves for join 1 |
| **Join C** — no hints, AQE + Photon decide | **4.8s** | `PhotonBroadcastHashJoin` + `PhotonShuffledHashJoin`, zero sorts, fully Photon — **6.8× vs A** |
| Write joined table (16 cols, LEFT joins) | 34.5s | |
| Write feature table (3 window cols over 59M rows) | 35.3s | one shuffle by (store, item) + in-partition sort |
| Window features, aligned spec (forced eval, warm) | **6.0s** | 1 shuffle, 1 sort |
| Window features, one window ordered by `d` instead of `date` | 8.9s | 1 shuffle, **2 sorts** (+48%) — and wrong on 29.4% of rows |

### What the plans show

**Join A** (forced sort-merge) — four `Exchange hashpartitioning` nodes: `long` by `d`, `cal` by `d`, the join result by `(store_id, item_id, wm_yr_wk)`, `prices` by the same. Every exchange is followed by a `Sort`. Photon reports `Unsupported node: SortMergeJoin` — the join runs on the JVM.

**Join B** (broadcast calendar) — join 1 becomes `PhotonBroadcastHashJoin … BuildRight`. `long` reads straight from `PhotonScan` into the join with no exchange in between; `cal` goes through `EXECUTOR_BROADCAST / SinglePartition`. Photon also pushes the broadcast keys into the Parquet scan as `hashedrelationcontains(d)` — rows whose `d` isn't in the calendar are filtered before they're materialized. Two exchanges remain, both for join 2.

**Join C** (no hints) — join 2 becomes `PhotonShuffledHashJoin … BuildRight` (`prices` is the build side). Both sides still shuffle — unavoidable, both are large — but there is no sort, and the plan reads *"The query is fully supported by Photon."*

## Trade-offs

- **Explicit schema over `inferSchema`.** Inferring 1,947 column types costs a full extra pass over the file (3.5s → 1.2s). A production pipeline declares its contract; it doesn't guess it.

- **`repartition("store_id")` before `partitionBy` — works, but doesn't matter at this scale.** Without it, each store directory got 1–2 files (the source CSV is already physically ordered by store and reads into only 8 partitions). With it, exactly 1 file — at the cost of a 59M-row shuffle (6.2s → 9.8s). Each store compresses to ~5 MB, so file count is irrelevant here. The pattern pays off when per-partition data is in the hundreds of MB and upstream partitions are many and unsorted. Measured; kept the 1-file layout since it was already written.

- **Broadcast the dimension, not the fact.** Sort-merge joining a 1,969-row calendar forced a 59M-row shuffle and sort. Broadcasting it: 32.7s → 10.0s. The 59M rows never leave their partitions for that join.

- **Hints change the engine, not just the algorithm.** Forcing `SortMergeJoin` didn't only add shuffles — it kicked that node out of Photon onto the JVM. With no hints, AQE chose `ShuffledHashJoin` for the 6.8M-row `prices` side: no sort, fully Photon, 10.0s → 4.8s. Know how to broadcast manually; know when the optimizer already knows better.

- **Inner join silently drops 20.8% of rows.** `prices` only covers weeks an item was on shelf, so an inner join discards 12,299,413 pre-launch item-days with no warning. Verified they are genuinely pre-launch — total sales on those rows is exactly 0 — then kept them via LEFT join with an `is_listed` flag. Whether to exclude them is an analysis decision, not a pipeline decision.

- **Same `partitionBy` → one shuffle; different `orderBy` → one sort each.** All three features partition by `(store_id, item_id)`, so Spark shuffles once regardless. Ordering one window by the string key `d` instead of `date` adds a second in-partition sort of 59M rows (6.0s → 8.9s, +48%) — and is also wrong: `'d_1000'` sorts after `'d_100'`, not after `'d_999'`, so `lag_7` disagrees with the correct value on **17,390,500 rows (29.4%)**. Align window specs; order by real types.

- **`count()` is not a benchmark for lazy columns.** The optimizer prunes window expressions nothing reads, so `df.count()` "timed" the window features at 0.6s while the write took 35s. Force computation by aggregating the outputs, and discard the first (cold-cache) run.

- **Serverless has no RDD API.** `df.rdd.getNumPartitions()` raises `RDD_NOT_SUPPORTED`; use `spark_partition_id().distinct().count()` instead.

## Correctness checks

**Pre-launch rows really are pre-launch.** Grouping the LEFT-joined table by `is_listed`:

```
+---------+----------+-----------+
|is_listed|      rows|total_sales|
+---------+----------+-----------+
|    false|12,299,413|          0|
|     true|46,881,677| 66,927,173|
+---------+----------+-----------+
```

**The misaligned window is wrong, not just slow.** `HOBBIES_1_001 @ CA_1` at the `d_999 → d_1000` boundary. `lag_ok` is ordered by `date`; `lag_bad` by the string `d`. Under lexical order `d_1000` sits near the top of the series (after `d_100`), so its "7 days prior" don't exist:

```
+----------+------+-----+------+-------+
|      date|     d|sales|lag_ok|lag_bad|
+----------+------+-----+------+-------+
|2013-10-21| d_997|    0|     1|      1|
|2013-10-22| d_998|    0|     0|      0|
|2013-10-23| d_999|    0|     0|      0|
|2013-10-24|d_1000|    0|     0|   NULL|
|2013-10-25|d_1001|    2|     0|   NULL|
|2013-10-26|d_1002|    2|     1|   NULL|
+----------+------+-----+------+-------+
```

Across the full table the two lag columns disagree on 17,390,500 of 59,181,090 rows (29.4%).

`sales_28d_avg` is a trailing window (`rowsBetween(-27, 0)`), so the first 27 listed days average over fewer than 28 points. `sales_lag_7` is NULL for the first 7 rows of each series. `days_since_listed` is NULL before listing and starts at 0 (verified: min = 0 over all listed rows).

## Running it

1. Databricks Free Edition → Catalog → Volume `workspace.default.m5` → upload the three Kaggle CSVs.
2. Import `notebooks/*.py` as notebooks (File → Import).
3. Run 01 → 03 in order. 02 is independent once 01 has written `out/sales_long`.

Outputs land under `/Volumes/workspace/default/m5/out/`.

## Not done (and why)

- **No bucketing.** Bucketing `long` and `prices` on `(store_id, item_id, wm_yr_wk)` would remove join 2's shuffle entirely. Skipped: it only pays off if the same join runs repeatedly, and M5 is a one-shot benchmark.
- **No Delta / OPTIMIZE.** Plain Parquet keeps the file-layout experiment honest; Delta's auto-compaction would hide it.
- **No skew handling.** M5 is perfectly balanced across stores (5,918,109 rows each). Real retail data isn't; salting or AQE skew-join would be the next thing to add.

## Stack

PySpark 3.5 · Databricks serverless (Photon) · Parquet / snappy · Unity Catalog volumes
