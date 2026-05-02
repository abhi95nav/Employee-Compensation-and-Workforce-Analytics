# Employee Compensation & Workforce Analytics

> End-to-end data engineering and analytics pipeline built on **Azure Databricks**, transforming raw HR/payroll CSVs into interactive executive dashboards using the **Medallion Architecture** (Bronze → Silver → Gold).

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Pipeline Notebooks](#pipeline-notebooks)
  - [Bronze Layer — Raw Ingestion](#bronze-layer--raw-ingestion)
  - [Silver Layer — Star Schema Transformation](#silver-layer--star-schema-transformation)
  - [Gold Layer — Analytics Aggregation](#gold-layer--analytics-aggregation)
  - [Business Cases — Scenario Analysis](#business-cases--scenario-analysis)
- [Gold Tables Reference](#gold-tables-reference)
- [Dashboard](#dashboard)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup & Configuration](#setup--configuration)
- [Usage](#usage)
- [Author](#author)

---

## Project Overview

School districts generate vast amounts of HR and payroll data — employee records, compensation breakdowns, certifications, and benefit plans — but rarely have the analytical infrastructure to answer strategic questions like:

- *Who are the highest-paid employees per district, and are they in the right roles?*
- *Which senior teachers are underpaid relative to their peers, putting them at flight risk?*
- *How many teachers have expired certifications, and what's the compliance exposure?*
- *Does a Master's degree actually pay off in terms of compensation?*

This project builds a complete **data lakehouse pipeline** that ingests 13 raw CSV files, cleanses and models them into a dimensional star schema, and produces 15 analytics-ready gold tables — all surfaced through a 5-page interactive Databricks dashboard.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Azure Data Lake Storage Gen2                         │
│                   abfss://employee@dataanlysisazuredatalake                 │
├──────────────────┬──────────────────┬──────────────────┬────────────────────┤
│   /source/       │   /bronze/       │   /silver/       │     /gold/         │
│   13 CSV files   │   13 Delta tbls  │   8 Delta tbls   │   15 Delta tbls    │
│                  │                  │   (star schema)  │   (denormalized)   │
└────────┬─────────┴────────┬─────────┴────────┬─────────┴──────────┬─────────┘
         │                  │                  │                    │
         ▼                  ▼                  ▼                    ▼
   ┌───────────┐     ┌───────────┐     ┌────────────┐      ┌─────────────┐
   │  employee  │────▶│  employee  │────▶│  employee   │─────▶│  employee   │
   │  _bronze   │     │  _silver   │     │  _gold      │      │  _business  │
   │            │     │            │     │             │      │  _cases     │
   └───────────┘     └───────────┘     └────────────┘      └──────┬──────┘
                                                                   │
                                                                   ▼
                                                          ┌──────────────┐
                                                          │  Databricks  │
                                                          │  Dashboard   │
                                                          │  (5 pages)   │
                                                          └──────────────┘

   Unity Catalog: employeedatacatalog
   Schemas:       bronze_employee  │  silver_employee  │  gold_employee
```

---

## Data Sources

The pipeline ingests **13 CSV files** from an Azure Data Lake source container:

| Category | Files | Description |
|----------|-------|-------------|
| **Core Entities** | `Employee`, `District`, `Admin`, `Teacher`, `Other_Employee` | Employee demographics, district info, role classifications |
| **Compensation** | `Total_PAB`, `PAB_Lineitem`, `PAB_item`, `REG_pay`, `OT_Pay`, `Benefit` | Pay-and-benefits totals, line items, regular/overtime/benefit breakdowns |
| **Certifications** | `Certification`, `Teacher_Cert` | Certification codes and teacher certification event records |

---

## Pipeline Notebooks

### Bronze Layer — Raw Ingestion
**Notebook:** `employee_bronze`

Reads all 13 CSVs with schema inference and appends four audit columns for traceability:

| Audit Column | Purpose |
|-------------|---------|
| `_bronze_ingested_at` | Processing timestamp |
| `_bronze_source_file` | Full ADLS path of source CSV |
| `_bronze_batch_id` | Unique UUID per row for batch tracking |
| `_bronze_is_valid` | Non-null check on the primary column |

**Output:** 13 Delta tables in `employeedatacatalog.bronze_employee`

---

### Silver Layer — Star Schema Transformation
**Notebook:** `employee_silver`

Cleanses bronze data into a proper **star schema** with typed columns, filtered invalid rows, and derived business attributes.

**Dimensions:**

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `dim_employee` | `EMP_ID`, `EMPLOYEE_NAME`, `years_of_service`, `seniority_level`, `promotion_eligible` | Derives tenure, seniority bands, and promotion eligibility |
| `dim_date_employee` | `date_key`, `day`, `month`, `quarter`, `year`, `is_weekend` | Built from union of all dates across the domain |
| `dim_district` | `DISTRICT_ID`, `DISTRICT_NAME`, `SUPERINTENDENT_ID` | Handles "NULL" string → actual null conversion |
| `dim_certification` | `CERT_ID`, `STATE_CERT_CODE`, `CERT_DESC` | Certification code lookup |
| `dim_pab_item` | `PAB_ITEM_ID`, `TYPE`, `item_category`, `taxable_code` | Merges Benefit + REG_pay + OT_pay into one dimension |

**Facts:**

| Table | Grain | Key Columns |
|-------|-------|-------------|
| `fact_pay_and_benefits` | One row per employee per tax year | `REG_PAY`, `OVERTIME_PAY`, `OTHER_PAY`, `TOTAL_BENEFITS`, `total_compensation` |
| `fact_pab_lineitem` | Individual pay/benefit line items | `AMOUNT_POSTED`, `BEG_DATE`, `END_DATE`, `POSTED_TIMESTAMP` |
| `fact_teacher_certification` | Teacher × certification events | `DATE_EFFECTIVE`, `DATE_EXPIRES`, `is_active`, `cert_duration_days` |

**Output:** 8 Delta tables in `employeedatacatalog.silver_employee`

---

### Gold Layer — Analytics Aggregation
**Notebook:** `employee_gold`

Joins silver dimensions and facts into wide, denormalized views optimized for analytics.

| Gold Table | Description |
|-----------|-------------|
| `gold_employee_summary` | One row per employee with demographics, compensation, tenure, and role flags |
| `gold_district_compensation` | District-level headcounts, avg pay, and promotion pipeline metrics |
| `gold_seniority_analysis` | Compensation ranges and role mix by seniority band |
| `gold_certification_overview` | Teacher cert records with Active/Expired/Unknown status labels |

**Output:** 4 Delta tables in `employeedatacatalog.gold_employee`

---

### Business Cases — Scenario Analysis
**Notebook:** `employee_business_cases`

Builds **11 self-contained business scenarios**, each producing a dedicated gold table:

| # | Scenario | Key Metric | Output Table |
|---|----------|------------|-------------|
| 1 | Top 10 Highest Compensated per District | Compensation Rank | `top10_compensated_per_district` |
| 2 | Promotion Pipeline Analysis | Eligible Count by Seniority | `promotion_pipeline` |
| 3 | Admin vs Teacher Pay Equity | Pay Gap % | `admin_vs_teacher_pay_equity` |
| 4 | Overtime Cost Hotspots | OT % of Regular Pay | `overtime_cost_hotspots` |
| 5 | Certification Gap (Compliance Risk) | Days Since Expiry | `certification_gap_report` |
| 6 | Underpaid Senior Employees | Comp Gap vs District Median | `underpaid_senior_employees` |
| 7 | Degree ROI Analysis | Premium vs Bachelor Baseline | `degree_roi_analysis` |
| 8 | Quarterly Pay Breakdown | Amount by Pay Type & Quarter | `quarterly_pay_breakdown` |
| 9 | Benefits Cost Optimization | Benefits % of Total Comp | `benefits_cost_optimization` |
| 10 | Retirement Risk & Workforce Planning | Headcount at Risk by Band | `retirement_risk_analysis` |
| 11 | Teacher Certification Breadth | Cert Count & Compensation Correlation | `teacher_certification_breadth` |

**Output:** 11 Delta tables in `employeedatacatalog.gold_employee`

---

## Gold Tables Reference

**Total: 15 gold Delta tables** across two notebooks, all registered in `employeedatacatalog.gold_employee`.

```sql
-- Query any gold table directly
SELECT * FROM employeedatacatalog.gold_employee.gold_employee_summary LIMIT 10;
SELECT * FROM employeedatacatalog.gold_employee.admin_vs_teacher_pay_equity;
SELECT * FROM employeedatacatalog.gold_employee.retirement_risk_analysis;
```

---

## Dashboard

**Name:** Employee Business Cases Dashboard
**Type:** Databricks Lakeview Dashboard (AI/BI)
**Data Sources:** 13 datasets pulling from the gold layer

### Pages

| Page | Widgets | Highlights |
|------|---------|-----------|
| **Executive Overview** | Total Employees, Avg Compensation, Imminent Retirees, Seniority Distribution, District Comparison | High-level KPIs and workforce snapshot |
| **Workforce Planning** | Retirement Risk by District, Seniority × District Matrix, Underpaid Senior Employees | Succession planning and retention risk |
| **Compensation Analysis** | Avg Comp by Role & District, Quarterly Pay by Type, Top 10 Compensated | Pay structure deep-dive |
| **Compliance & Operations** | Benefits Cost Distribution, Overtime Hotspots, Certification Gaps | Operational risk monitoring |
| **Global Filters** | District, Seniority Level, Degree Level | Cross-page interactive filters |

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Cloud Platform** | Microsoft Azure |
| **Compute** | Azure Databricks (Serverless) |
| **Storage** | Azure Data Lake Storage Gen2 (ADLS) |
| **Data Format** | Delta Lake |
| **Data Catalog** | Unity Catalog |
| **Processing** | PySpark (Python) |
| **Visualization** | Databricks AI/BI Dashboards (Lakeview) |
| **Architecture** | Medallion (Bronze → Silver → Gold) |

---

## Project Structure

```
Employee-Overview/
│
├── README.md                                    # This file
├── employee_bronze.ipynb                        # Bronze layer — raw CSV ingestion
├── employee_silver.ipynb                        # Silver layer — star schema transformation
├── employee_gold.ipynb                          # Gold layer — analytics aggregation
├── employee_business_cases.ipynb                # 11 business case scenarios
└── Employee Business Cases Dashboard.lvdash.json  # Databricks Lakeview dashboard
```

---

## Setup & Configuration

### Prerequisites

- Azure Databricks workspace with Unity Catalog enabled
- Azure Data Lake Storage Gen2 account (`dataanlysisazuredatalake`)
- Storage container named `employee` with source CSVs in the `/source/` folder

### Configuration

All notebooks share the same configuration block:

```python
# Storage paths
root_path = "abfss://employee@dataanlysisazuredatalake.dfs.core.windows.net/"
bronze_path = f"{root_path}/bronze"
silver_path = f"{root_path}/silver"
gold_path   = f"{root_path}/gold"

# Unity Catalog references
employee_db = "employeedatacatalog"
bronze_sch  = "bronze_employee"
silver_sch  = "silver_employee"
gold_sch    = "gold_employee"
```

### Execution Order

Run the notebooks in sequence — each layer depends on the previous:

```
1. employee_bronze       →  Ingests CSVs into bronze Delta tables
2. employee_silver       →  Transforms bronze into silver star schema
3. employee_gold         →  Aggregates silver into gold analytics views
4. employee_business_cases →  Builds 11 scenario-specific gold tables
5. Dashboard             →  Automatically refreshes from gold tables
```

---

## Usage

1. **Clone this repo** into your Databricks workspace
2. **Attach a cluster** (or use serverless compute)
3. **Run notebooks 1–4** in order to build the full pipeline
4. **Import the dashboard** (`Employee Business Cases Dashboard.lvdash.json`) to visualize results
5. **Query gold tables** directly for ad-hoc analysis:

```sql
-- Which districts have the worst admin-teacher pay gap?
SELECT DISTRICT_NAME, pay_gap_pct, equity_flag
FROM employeedatacatalog.gold_employee.admin_vs_teacher_pay_equity
ORDER BY pay_gap_pct DESC;

-- Who's at highest retention risk?
SELECT EMPLOYEE_NAME, DISTRICT_NAME, comp_gap_vs_median, retention_risk
FROM employeedatacatalog.gold_employee.underpaid_senior_employees
WHERE retention_risk = 'Critical';
```

---

## Author

**Athupili** — Kent State University
Built on Azure Databricks | May 2026

---
