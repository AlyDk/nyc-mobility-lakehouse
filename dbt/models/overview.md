{% docs \_\_overview\_\_ %}

\# nyc-mobility-lakehouse



Fleet operations analytics on NYC TLC yellow and green taxi trips, 2023-2024, built on Databricks.



\*\*Flow:\*\* TLC monthly Parquet → Bronze (Delta, per-month idempotent loads, manifest) → Silver (conformed, DQ flags, MERGE incremental) → Gold (this dbt project).



\*\*Gold layer\*\*

\- `fct\_trips`: 78,270,141 valid trips, one row per trip

\- `agg\_trips\_zone\_hour`: 5.5M rows, Power BI import grain (day, hour, zone)

\- `dim\_date`, `dim\_zone`, `dim\_payment\_type`



\*\*Quality gates:\*\* key, relationship and accepted-value tests, plus reconciliation tests that prove the fact equals the valid Silver trips and the aggregate equals the fact to the cent.



Source code: https://github.com/AlyDk/nyc-mobility-lakehouse

{% enddocs %}

