## Data
[M5 Forecasting – Accuracy](https://www.kaggle.com/competitions/m5-forecasting-accuracy) (Kaggle)
- `sales_train_evaluation.csv` — 30,490 × 1,947 (wide)
- `calendar.csv` — 1,969 rows
- `sell_prices.csv` — 6,841,121 rows
- After reshape: **59,181,090 rows → ~50 MB snappy Parquet**, 10 partitions (5,918,109 rows each, no skew)

## What it does
- [x] Load raw CSVs with an explicit schema — `notebooks/01_load_and_melt.py`
- [x] Reshape wide → long with `stack()` → 59.2M rows
- [x] Write Parquet partitioned by `store_id`; measure file layout with/without `repartition`
- [x] Join calendar + prices; benchmark sort-merge vs. broadcast vs. AQE — `notebooks/02_join_benchmark.py`
- [ ] Rolling 28-day features with window functions — `notebooks/03_window_features.py`
## Benchmark (Databricks serverless, Photon)
| Step | Time | Notes |
|---|---|---|
| Read 30,490 × 1,947 CSV, `inferSchema=True` | 3.5s | full extra pass to infer 1,947 column types |
| Same read, explicit schema | **1.2s** | 2.9× |
| `stack()` to 59.2M rows + count | 5.3s | narrow, no shuffle |
| Write `partitionBy("store_id")` | 6.2s | 1–2 files/store |
| Write `repartition` + `partitionBy` | 9.8s | 1 file/store; extra shuffle |
| **Join A** — 2× sort-merge (forced by hint) | 32.7s | 4 shuffles, 2 sorts, falls out of Photon |
| **Join B** — broadcast `cal` + sort-merge `prices` | 10.0s | 2 shuffles; 59M-row side never moves for join 1 |
| **Join C** — no hints, AQE + Photon decide | **4.8s** | `PhotonBroadcastHashJoin` + `PhotonShuffledHashJoin`, no sorts, fully Photon — **6.8× vs A** |

## Trade-offs
- **Explicit schema over `inferSchema`** — 3.5s → 1.2s. Declare the contract, don't guess it.
- **`repartition` before `partitionBy` — works, but doesn't matter at this scale.** 1–2 → 1 file/store
  for a 59M-row shuffle (6.2s → 9.8s); each store is ~5 MB so file count is irrelevant here.
- **Broadcast the dimension, not the fact.** `cal` is 1,969 rows; sort-merge joining it forced a
  59M-row shuffle + sort. Broadcasting it: 32.7s → 10.0s. Photon also pushes the broadcast keys
  into the Parquet scan as a filter (`hashedrelationcontains`).
- **Hints change the engine, not just the algorithm.** Forcing `SortMergeJoin` kicked that node
  out of Photon (unsupported) onto the JVM. With no hints, AQE picked `ShuffledHashJoin` for the
  6.8M-row `prices` side — no sort, fully Photon — 10.0s → 4.8s. Know how to broadcast manually;
  know when to let the optimizer decide.
- **Inner join silently drops 20.8% of rows** — `prices` only covers weeks an item was listed.
  Canonical output uses LEFT joins and an `is_listed` flag instead.
- **Serverless has no RDD API** — use `spark_partition_id()` instead of `rdd.getNumPartitions()`.