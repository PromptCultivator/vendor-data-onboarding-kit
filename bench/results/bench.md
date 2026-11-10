# Benchmark Results

Values report median and min–max range across isolated subprocess runs.

| Metric | Synthetic (50 MB) | Synthetic (1 GB) | Synthetic (10 GB) | Synthetic (50 GB) | SEC FSDS (2023q3–2023q4) |
|---|---|---|---|---|---|
| Time from raw drop to report | 8.27 s (7.96–8.45 s) | 56.69 s (55.54–56.81 s) | TBD | TBD | 68.81 s (68.13–88.27 s) |
| Validation throughput | 0.029 GB/s/core (0.028–0.030; 675,641 rows/s, cores=2) | 0.037 GB/s/core (0.036–0.039; 849,928 rows/s, cores=2) | TBD | TBD | 0.053 GB/s/core (0.048–0.053; 634,408 rows/s, cores=2) |
| Store write throughput | 937,370 rows/s (67.4 MB/s, 64.3–68.9) | 1,644,264 rows/s (123.6 MB/s, 77.3–133.5) | TBD | TBD | 421,645 rows/s (41.4 MB/s, 32.6–42.1) |
| Peak resident memory | 630.1 MiB (606.0–653.8 MiB) | 4967.0 MiB (4892.8–5053.1 MiB) | TBD | TBD | 4080.0 MiB (4070.8–4142.9 MiB) |
| Revision detection recall and precision | recall 100.0%, precision 100.0% (25 revisions) | recall 100.0%, precision 100.0% (25 revisions) | TBD | TBD | 15,518 revisions detected (unlabeled) |
| Point-in-time violations | 0 / 100,000 | 0 / 100,000 | TBD | TBD | 0 / 100,000 |
| Leak detector recall | 100.0% (1,000/1,000, 0 false positives) | 100.0% (1,000/1,000, 0 false positives) | TBD | TBD | 100.0% (1,000/1,000, 0 false positives) |
| Storage factor | 0.585× (0.585–0.586×) | 0.580× | TBD | TBD | 0.581× (0.580–0.581×) |

## Per-Stage Wall-Clock Breakdown (Median)

| Stage | Synthetic (50 MB) | Synthetic (1 GB) | SEC FSDS (2023q3–2023q4) |
|---|---|---|---|
| Discovery | 0.252 s (0.249–0.258 s) | 1.322 s (1.258–1.384 s) | 1.765 s (1.758–5.766 s) |
| Schema | 3.910 s (3.755–4.059 s) | 5.038 s (4.876–5.171 s) | 6.714 s (6.459–7.407 s) |
| Normalization | 2.003 s (1.945–2.064 s) | 28.193 s (27.135–28.681 s) | 18.078 s (17.907–26.208 s) |
| Validation | 0.611 s (0.584–0.629 s) | 9.934 s (9.331–10.087 s) | 10.943 s (10.852–12.117 s) |
| Store write | 0.441 s (0.431–0.462 s) | 5.136 s (4.758–8.210 s) | 16.465 s (16.202–20.942 s) |
| Revisions | 0.419 s (0.403–0.422 s) | 4.296 s (3.908–4.363 s) | 9.022 s (8.899–9.535 s) |
| Reporting | 0.311 s (0.304–0.326 s) | 1.692 s (1.623–1.758 s) | 2.104 s (2.058–2.123 s) |
| **Total** | 8.27 s (7.96–8.45 s) | 56.69 s (55.54–56.81 s) | 68.81 s (68.13–88.27 s) |

## Hardware and Environment

```
Hardware & Environment:
  CPU Model: Intel(R) Xeon(R) Platinum 8481C
  CPU Cores: 2
  Memory: 7.8 GiB
  Disk Type: Virtual block store (virtio_blk)
  Operating System: Linux-6.12.105+-x86_64-with-glibc2.36
  Python Version: 3.12.16
  DuckDB Version: 2.0.0
  PyArrow Version: 26.0.1
  Git Commit: 84a7f040b8912605b127413e1a0ae6b60615df79
```

## Notes

- Each iteration executes inside a fresh Python subprocess so `resource.getrusage` captures an independent resident memory high-water mark.
- Synthetic sequences comprise 3 drops (`drop_1` baseline, `drop_2` with planted duplicates, type flips, out-of-range prices, and 25 restatements, and `drop_3` with schema column removal held at the schema gate). Storage factor divides vendor store bytes by total raw drop sequence bytes.
