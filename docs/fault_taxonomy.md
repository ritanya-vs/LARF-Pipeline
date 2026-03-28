# Fault Taxonomy

This document maps injected faults to the detectors that identify them.

| Fault Type        | Injected Behavior                               | Detector              | Signal |
|-------------------|--------------------------------------------------|----------------------|--------|
| Data Quality      | heart_rate > 250, spo2 < 60                    | zscore_detector      | z > 3 |
| Data Quality      | distribution shift across batch                 | ks_test              | p < 0.05 |
| Schema Fault      | missing or extra fields                         | schema_entropy       | distance > threshold |
| Performance Fault | latency spike                                   | zscore_detector      | z > 3 |
| Pipeline Stall    | consumer lag spike                              | zscore_detector      | z > 3 |
| Security Fault    | unusual multi-metric behavior                   | isolation_forest     | anomaly score |