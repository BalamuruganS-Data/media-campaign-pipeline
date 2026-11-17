# Media Campaign Performance Pipeline

End-to-end data engineering project on **Databricks**: API extraction from Kaggle, a **medallion architecture** (bronze → silver → gold), a **star schema** with correctly calculated advertising KPIs, **automated data quality checks**, and a **finance billing reconciliation** that catches real-world invoice discrepancies.

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD4?style=flat&logo=delta&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=databricks&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![Azure Data Factory](https://img.shields.io/badge/Azure%20Data%20Factory-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![GitHub Pages](https://img.shields.io/badge/Live%20dashboard-GitHub%20Pages-222222?style=flat&logo=github&logoColor=white)

### [Open the live dashboard](https://balamurugans-data.github.io/media-campaign-pipeline/)

A public web dashboard that reads the pipeline's latest verified export: market trends, client and media-channel breakdowns, all-platform KPIs, the billing reconciliation and the data quality history.
![Live dashboard](https://img.shields.io/badge/Live%20dashboard-online-2F6FDE?style=flat&logo=githubpages&logoColor=white)

**[Open the live dashboard](https://balamurugans-data.github.io/media-campaign-pipeline/)**, refreshed from the pipeline's latest export.

---

## Contents

- [The business problem](#the-business-problem)
- [Architecture](#architecture)
- [Data sources](#data-sources)
- [Pipeline layers](#pipeline-layers)
- [Star schema](#star-schema)
- [KPIs and the averaging trap](#kpis-and-the-averaging-trap)
- [Data quality](#data-quality)
- [Finance reconciliation](#finance-reconciliation)
- [Live dashboard](#live-dashboard)
- [Orchestration](#orchestration)
- [Key engineering decisions](#key-engineering-decisions)
- [Data limitations](#data-limitations)
- [Repository structure and run order](#repository-structure-and-run-order)
- [Power BI report](#power-bi-report)
- [Roadmap](#roadmap)

---

## The business problem

Media agencies receive daily delivery data from several advertising platforms. Each platform uses different file formats, column names and quirks. The data team has to combine them, clean them, calculate performance metrics correctly, and reconcile what platforms report against what finance is actually billed, before any number reaches a client.

This project builds that pipeline from scratch.

---

## Architecture

```mermaid
flowchart LR
    K[Kaggle API - 3 public datasets] -->|Python extraction| BV[(Bronze volume - date-partitioned CSVs)]
    F[Finance billing feed - simulated] --> BV
    BV --> BT[Bronze Delta tables - raw text plus lineage]
    BT --> S1[silver_daily]
    BT --> S2[silver_social]
    BT --> S3[silver_multichannel]
    BT --> S4[silver_finance_billing]
    S1 --> SU[silver_campaign_performance - unified]
    S2 --> SU
    S3 --> SU
    SU --> G[Gold star schema]
    G --> KPI[Platform KPI view]
    G --> REC[Billing reconciliation view]
    S4 --> REC
    SU --> DQ[Automated data quality checks]
    G --> DQ
    G --> PUB[Publish notebook - validated JSON export]
    REC --> PUB
    DQ --> PUB
    PUB -->|GitHub API| WEB[Live dashboard on GitHub Pages]
    G --> PBI[Power BI report]
    G --> PUB[Publish notebook - validated JSON export]
    REC --> PUB
    DQ --> PUB
    PUB -->|GitHub API| WEB[Live dashboard on GitHub Pages]
```

| Layer | Purpose | Rule |
|---|---|---|
| **Bronze** | Data exactly as received | Never modified; everything stored as text, plus `_source_file` and `_ingested_at` for lineage |
| **Silver** | Cleaned, typed, standardised | Every transformation reads from bronze and writes a new table |
| **Gold** | Modelled for analysis | Star schema and KPI logic, built only from silver |

---

## Data sources

| Role in project | Dataset | Rows | Notes |
|---|---|---|---|
| Social platform | Kaggle: Clicks Conversion Tracking | 1,143 | Ad-level Facebook campaign data; **no date column** |
| Daily campaign platform | Kaggle: A/B Testing dataset | 60 | Daily control and test campaigns, August 2019; semicolon-delimited |
| Multichannel platform | Kaggle: Marketing Campaign Performance | 200,000 | **Fictional dataset**, used for volume; one row per campaign, 2021 |
| Finance billing | Generated from delivered spend | 15 | **Simulated**, with five documented discrepancies (see [reconciliation](#finance-reconciliation)) |

Extraction uses the Kaggle API from a Databricks notebook. The API token is stored in a Unity Catalog volume and read at runtime; it is never written into code or committed to this repository.

---

## Pipeline layers

<details>
<summary><b>Bronze: extraction and raw landing</b> (click to expand)</summary>

- Python extraction from the Kaggle API into date-partitioned folders: `bronze/<platform>/ingest_date=YYYY-MM-DD/`
- Downloads go to a temporary folder first and are copied to bronze only on success, so a failed download never lands half-written files
- **Error isolation:** one failing source does not stop the others
- A **manifest** records each run's files, row counts and status
- Raw files loaded into bronze Delta tables as text, with only a technical rename of characters Delta does not allow in column names
- Extraction is **idempotent**: rerunning overwrites the same day's files without creating duplicates
- **Snapshot loading:** each run lands a full copy of every source in a new dated folder, and bronze tables load only the latest one, so scheduled daily runs never stack copies, while older snapshots stay available for audit

</details>

<details>
<summary><b>Silver: cleaning and standardisation</b> (click to expand)</summary>

Issues found during profiling, and how each was handled:

| Source | Issue | Handling |
|---|---|---|
| Daily | Semicolon delimiter | Explicit `sep` per source |
| Daily | Date format `d.MM.yyyy` | Explicit date parsing |
| Daily | 5 Aug 2019 has spend but no performance data | Kept, flagged `is_incomplete` |
| Daily | Purchases above add-to-carts on some days | Kept, flagged `has_funnel_anomaly` (9 days) |
| Social | 204 ads with conversions but zero clicks | Kept as **view-through conversions**, flagged `is_view_through` |
| Multichannel | Cost stored as text like `$16,174.00` | Symbols removed before conversion |
| Multichannel | Duration stored as `30 days` | Number extracted |

The three source tables are combined with `UNION ALL` into `silver_campaign_performance`, using a shared naming standard and typed nulls where a source lacks a column.

</details>

<details>
<summary><b>Gold: star schema and views</b> (click to expand)</summary>

- `gold_dim_date`, `gold_dim_platform`, `gold_dim_campaign`, `gold_fact_campaign_performance`
- `gold_vw_platform_kpis`: KPIs calculated from totals
- `gold_vw_billing_reconciliation`: delivered spend against billed amounts

**Fact grain:** one row per platform record as delivered by the source (a day, an ad, or a campaign summary).

</details>

---

## Star schema

```mermaid
erDiagram
    gold_dim_date ||--o{ gold_fact_campaign_performance : date_key
    gold_dim_platform ||--o{ gold_fact_campaign_performance : platform_key
    gold_dim_campaign ||--o{ gold_fact_campaign_performance : campaign_key

    gold_fact_campaign_performance {
        int date_key
        bigint platform_key
        bigint campaign_key
        decimal spend
        int impressions
        int clicks
        int conversions
        boolean is_incomplete
        boolean is_view_through
        boolean has_funnel_anomaly
    }
    gold_dim_date {
        int date_key
        date full_date
        int year
        int quarter
        int month
        string month_name
        string day_name
        boolean is_weekend
    }
    gold_dim_platform {
        bigint platform_key
        string platform
        string platform_name
    }
    gold_dim_campaign {
        bigint campaign_key
        string platform
        string campaign_id
        string client
        string market
        string media_channel
        string campaign_type
    }
```

- **Surrogate keys** use `xxhash64`, which is deterministic, so keys stay stable across rebuilds and the fact table computes them without lookups
- **Campaign key = platform + campaign_id**, because campaign IDs are only unique within a platform
- **Client, market, media channel and campaign type** come from the multichannel source through a left join; the other sources have no such attributes and are labelled `Unassigned` rather than dropped
- **Unknown member:** `date_key = 0` represents undated social records, so they are never silently dropped by joins
- **Campaign attributes:** client, market, media channel and campaign type come from the multichannel source via a `LEFT JOIN`; daily and social campaigns are labelled `Unassigned` rather than dropped, and the campaign count was verified unchanged after the join
- Verified with an **orphan check**: zero fact rows without a matching dimension row

---

## KPIs and the averaging trap

| Metric | Formula |
|---|---|
| CTR | total clicks ÷ total impressions × 100 |
| CPC | total spend ÷ total clicks |
| CPM | total spend ÷ total impressions × 1,000 |
| CPA | total spend ÷ total conversions |

Ratios are **always calculated from totals, never averaged**. Averaging row-level CTRs gives small rows the same weight as large ones. Tested on this data:

| Platform | Average of row CTRs | CTR from totals | Error from averaging |
|---|---|---|---|
| Daily Campaign | 7.71% | 6.19% | overstated by about 25% |
| Multichannel | 14.04% | 9.98% | overstated by about 41% |
| Social | 0.016% | 0.018% | understated by about 11% |

Spend from the incomplete day is excluded from CPC, CPM and CPA (but kept in total spend), so the numerator and denominator of each ratio always cover the same rows.

<details>
<summary><b>Resulting KPIs per platform</b> (click to expand)</summary>

| Platform | Total spend | CTR | CPC | CPM | CPA |
|---|---|---|---|---|---|
| Daily Campaign | 145,545.00 | 6.19% | 0.43 | 26.54 | 4.67 |
| Social | 58,705.23 | 0.018% | 1.54 | 0.28 | 17.99 |
| Multichannel | 2,500,878,608.00 | 9.98% | 22.74 | 2,270.51 | not available |

These come from three unrelated datasets with different definitions of a click, so they should not be compared across platforms. See [data limitations](#data-limitations).

</details>

---

## Data quality

Six automated checks run as one snapshot and append their results to `dq_results`, building a history of pipeline health:

| Check | Layer | Rule |
|---|---|---|
| `daily_rows_bronze_vs_silver` | silver | Row counts match |
| `social_rows_bronze_vs_silver` | silver | Row counts match |
| `multichannel_rows_bronze_vs_silver` | silver | Row counts match |
| `null_dates_non_social` | silver | Only social may lack dates |
| `spend_silver_vs_fact` | gold | Total spend matches to the cent |
| `orphan_campaigns` | gold | Every fact row has a campaign |

**Testing the tests:** a row was deliberately deleted from `silver_daily`. The row-count check failed (expected 60, found 59), the table was restored with Delta time travel (`RESTORE TABLE ... TO VERSION AS OF`), and the next run passed. The history shows the full incident: **PASS → FAIL → PASS**.

The same test exposed a coverage gap: the spend check still passed, because it compares two downstream tables that both still held the deleted row. A check only sees what it compares, so checks against the source are essential.

---

## Finance reconciliation

Platform-reported spend is matched against billed amounts per platform per month using a **full outer join**, so unmatched rows on either side become findings rather than disappearing. A $1 tolerance absorbs rounding.

The simulated billing feed contains five planted discrepancies. All five were caught:

| Planted problem | Result | Action for finance |
|---|---|---|
| Daily, Aug 2019: invoice excludes the incomplete day | VARIANCE, −$1,835.00 | Verify whether that day was delivered |
| Multichannel, Mar 2021: overbilled by 2.5% | VARIANCE, +$5,297,315.08 | Dispute the overcharge |
| Multichannel, Dec 2021: invoice never received | DELIVERED_NOT_BILLED | Chase the missing invoice |
| Multichannel, Jul 2021: invoice sent twice | MATCH, duplicate flag set | Ensure it is paid only once |
| Multichannel, Jan 2022: invoice with no delivery | BILLED_NOT_DELIVERED | Reject the invoice |

**Why line-level matters:** at the total level, billed and delivered differ by about $5.07 million. That figure hides a **missing $212 million invoice** almost exactly cancelled out by a **duplicate $212 million invoice**. Checking totals alone would have missed both.

---

## Live dashboard

**[balamurugans-data.github.io/media-campaign-pipeline](https://balamurugans-data.github.io/media-campaign-pipeline/)**

| Tab | What it shows |
|---|---|
| **Market Trends** | Spend, impressions, clicks, CTR and CPC with change against the previous period; a clickable market treemap; monthly spend with month-over-month change; market summary |
| **Client Overview** | Spend by client and media channel with heat-map shading, in dollars or percentages |
| **Media Breakdown** | Market by channel splits, period-on-period change by market and channel, channel shares and summary |
| **Platforms & Trust** | All-platform KPIs, the daily campaign trend, what the billing total hides, the reconciliation and the quality check history |

<details>
<summary><b>How it stays live, and why it is safe</b> (click to expand)</summary>

1. The pipeline runs in Databricks.
2. `04_publish_dashboard_data` exports the latest gold results and quality checks as `docs/data/dashboard.json`, **validates** the export (spend must match the fact table to the cent, and breakdown spend must match multichannel spend), and commits it through the GitHub API.
3. GitHub Pages serves the dashboard, which reads that file on every visit and shows when it was refreshed.

The browser never connects to Databricks: no credentials are ever placed in a public page. If the export cannot be loaded, the page falls back to an embedded snapshot and says so.

</details>

**Honest labelling:** the data covers one year, so changes are always labelled with what they compare (for example "vs Q2" or "H2 vs H1") instead of a year-over-year figure that cannot exist. Market and channel shares are close to equal because the multichannel dataset is synthetic, and the page says so.

---

## Orchestration

The whole pipeline runs every day without manual steps: **Azure Data Factory** owns the schedule and triggers a **Databricks job** that runs the six notebooks in order.

```mermaid
flowchart LR
    T[ADF schedule trigger - daily 06:00 IST] --> P[ADF pipeline pl_run_media_pipeline]
    P -->|managed identity| KV[(Azure Key Vault - Databricks token)]
    P -->|Jobs API run-now| J[Databricks job]
    P -->|polls runs/get every 60s| J
    J --> N1[01 bronze and silver] --> N2[00 billing simulator] --> N3[01b billing] --> N4[02 gold] --> N5[03 data quality] --> N6[04 publish]
    N6 -->|GitHub API| GH[docs/data/dashboard.json] --> WEB[Live dashboard]
    J -.->|failure email| E[Alert]
```

<details>
<summary><b>Design decisions</b> (click to expand)</summary>

- **Why ADF triggers Databricks through the REST API:** the project runs on Databricks Free Edition, which is not an Azure Databricks workspace, so ADF's native notebook activity cannot reach it. Triggering a Databricks job through the Jobs API is a common enterprise pattern for compute that lives outside Azure.
- **One scheduler only:** the schedule lives in ADF; the Databricks job has none, so the pipeline never runs twice.
- **No secrets in the pipeline:** the token sits in Azure Key Vault and is read at runtime with the factory's managed identity, which holds only the read-only *Key Vault Secrets User* role. Secure input and output keep the token out of run logs. Rotating the token means updating one secret, with no pipeline change.
- **No retries on the start call:** if the request timed out after Databricks had already started the job, a retry would start a second run. Status checks do retry, because asking twice is harmless.
- **The orchestrator fails when the job fails:** a final check fails the ADF run unless Databricks reports `SUCCESS`. Otherwise ADF would show green whenever its API calls worked, even if the pipeline itself had failed.
- **Every layer stops bad data:** a failed task stops the Databricks job, and the publish notebook validates the export before committing, so the live dashboard keeps the last good data instead of showing broken numbers.
- **Snapshot-safe daily runs:** each run lands a new dated copy of every source, and bronze loads only the latest one, so repeated runs never stack duplicate data.

</details>

**Running cost:** Databricks Free Edition is free; one ADF run a day with its polling loop and the Key Vault reads costs well under a dollar a month, watched by an Azure budget alert.

The ADF pipeline and trigger definitions are version-controlled in [`adf/`](adf/) through ADF's GitHub integration.

---

## Key engineering decisions

<details>
<summary><b>Click to expand the decisions and the reasoning behind them</b></summary>

- **Profile before transforming.** Every silver rule traces back to a finding in profiling.
- **Flag, don't delete.** Incomplete rows, funnel anomalies, view-through conversions and duplicate invoices are kept and flagged, so problems stay visible.
- **Fail loudly.** `CAST` is used instead of `try_cast` in silver, so unexpected values stop the pipeline instead of becoming silent nulls.
- **Idempotent rebuilds.** Data tables use `CREATE OR REPLACE TABLE ... AS SELECT`; only the `dq_results` log is appended to, because its purpose is history.
- **Typed nulls in unions.** `CAST(NULL AS DATE)` instead of `NULL`, so the result does not depend on the order of the `SELECT`s.
- **Domain knowledge over tutorials.** Zero-click conversions are often deleted as illogical; in advertising they are legitimate view-through conversions.
- **Nulls are honest.** Multichannel conversions are null rather than estimated, because the source's conversion rate definition is unknown.
- **Debugging with evidence.** An unexpectedly empty table was traced with `DESCRIBE HISTORY` to the exact operation that emptied it.

</details>

---

## Data limitations

- The three platform datasets are unrelated and cover different periods (August 2019, all of 2021, and undated), so platforms cannot be compared over the same time range.
- Multichannel data is fictional: its CPM of over $2,000 is far beyond any real advertising price, so no business conclusions should be drawn from it.
- Multichannel has one row per campaign, so its dates are campaign dates, not daily delivery.
- `Acquisition_Cost` is interpreted as total campaign spend; the name is ambiguous.
- Social data has no dates and is reported as campaign-level totals.
- Multichannel markets, clients and channels each hold a near-equal share of spend, another sign the dataset is synthetic.
- Market, client and channel shares are almost exactly equal because the multichannel data is synthetic; real media mixes are far less even.
- The finance billing feed is simulated.

---

## Repository structure and run order

| Order | Notebook | Purpose |
|---|---|---|
| 1 | [`01_bronze_and_silver`](01_bronze_and_silver.ipynb) | Kaggle extraction, bronze tables (latest snapshot), silver tables for all platforms |
| 2 | [`00_simulate_finance_billing`](00_simulate_finance_billing.ipynb) | Source simulator: generates the billing feed from delivered spend |
| 3 | [`01b_finance_billing`](01b_finance_billing.ipynb) | Billing bronze and silver, duplicates flagged |
| 4 | [`02_gold`](02_gold.ipynb) | Star schema, KPI view, reconciliation view |
| 5 | [`03_data_quality`](03_data_quality.ipynb) | Automated checks appended to a results history |
| 6 | [`04_publish_dashboard_data`](04_publish_dashboard_data.ipynb) | Validated JSON export committed to GitHub for the live dashboard |

`docs/index.html` is the live dashboard and `docs/data/dashboard.json` is its data, refreshed by notebook 6. A Databricks job runs the six notebooks in this order, triggered daily by the ADF pipeline in [`adf/`](adf/).

---

## Power BI report

The same gold layer also feeds a Power BI report used for internal-style analysis: a star schema with many-to-one relationships, KPIs as DAX measures, and a reconciliation page. Building it surfaced two useful lessons:

- **A totals bug caught by checking the total row:** CPA showed $73,427 overall because total spend included multichannel while total conversions did not. Restricting CPA's spend to rows that report conversions fixed it to $5.94.
- **Auto-detected relationships needed correcting:** Power BI linked two dimensions on a text column, creating a second filter path and silently deactivating the fact-to-campaign relationship.

Every DAX measure was validated against the SQL KPI view. The report is being updated to use the new client, market and channel attributes.

---

## Roadmap

- [x] Checkpoint 1: Extraction and bronze layer
- [x] Checkpoint 2: Silver layer
- [x] Checkpoint 3: Gold layer star schema and KPIs
- [x] Checkpoint 4: Data quality checks and finance reconciliation
- [x] Checkpoint 5: Reporting in Power BI and a live public dashboard
- [x] Checkpoint 6: Daily orchestration with Azure Data Factory triggering a Databricks job
- [ ] Checkpoint 7: Portfolio finish

**Backlog**

- [x] Client attribute in the model (client, market, media channel and campaign type)
- [ ] Client-level row-level security in Power BI
- [ ] Extra quality check: unified silver row count against the three source tables
- [ ] Agentic Q&A assistant that answers questions like "how many clicks did this client get?" from the gold layer

---

## Author

**Balamurugan S** · Data Engineer · Chennai
[LinkedIn](https://linkedin.com/in/-bala-murugan) · [GitHub](https://github.com/BalamuruganS-Data)
