# GitPulse — End-to-End ELT Platform for GitHub Activity Analytics

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![dbt](https://img.shields.io/badge/dbt-Core%20v1.13-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.65-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Docker](https://img.shields.io/badge/Docker-29.1-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 💡 Elevator Pitch

**GitPulse** is an enterprise-grade, open-source ELT data platform designed to monitor, model, and visualize engineering velocity across open-source and proprietary GitHub repositories. Moving beyond fragile ETL scripts, GitPulse extracts high-fidelity GitHub REST entities (commits, pull requests, issues, and repository metadata), stages them as raw JSONB in PostgreSQL, and executes modular, tested transformations using dbt to produce analytics-ready marts. The transformed data powers a modern, high-performance Streamlit dashboard delivering real-time insights into cycle times, contributor schedules, merge distributions, and issue resolution velocities.

---

## 🏛️ System Architecture

```mermaid
flowchart TD
    subgraph Source["GitHub REST API v3 / 2022-11-28"]
        GH_API["GitHub API Endpoints<br/>(/repos, /commits, /issues, /pulls)"]
    end

    subgraph Extract["Extraction Layer (Python)"]
        CLIENT["GitHubClient<br/>(extract/github_client.py)"]
        STATE["State Manager<br/>(data/state.json)"]
        RETRY["Tenacity Exponential Backoff<br/>(4s ➔ 120s, max 5 attempts)"]
        RATELIMIT["Rate-Limit Monitor<br/>(Remaining < 50 ➔ Sleep until Reset + 5s)"]
        
        CLIENT --- RETRY
        CLIENT --- RATELIMIT
        STATE -. "since watermark" .-> CLIENT
    end

    subgraph Load["Staging / Storage Layer (PostgreSQL 16)"]
        RAW_REPOS[("raw_repos<br/>JSONB + GIN Index")]
        RAW_COMMITS[("raw_commits<br/>JSONB + GIN Index")]
        RAW_ISSUES[("raw_issues<br/>JSONB + GIN Index")]
        RAW_PULLS[("raw_pulls<br/>JSONB + GIN Index")]
        
        TX["Single Transaction Upsert<br/>(ON CONFLICT DO UPDATE)"]
    end

    subgraph Transform["Transformation Layer (dbt Core)"]
        STG_VIEWS["Staging Views (models/staging/)<br/>stg_repos, stg_commits,<br/>stg_issues, stg_pulls"]
        TESTS["Data Integrity Tests<br/>(unique, not_null, accepted_values)"]
        MARTS["Analytics Mart Tables (models/marts/)<br/>dim_repos, fct_commits,<br/>fct_pull_requests, fct_issues"]
    end

    subgraph Serve["Serving & Visualization Layer"]
        ENGINE["SQLAlchemy Engine<br/>(pool_pre_ping=True)"]
        STREAMLIT["Streamlit Analytics Dashboard<br/>(dashboard/app.py)"]
        P1["Page 1: Repository Overview"]
        P2["Page 2: Developer Activity"]
        P3["Page 3: Pull Request Health"]
        P4["Page 4: Issue Analytics"]
    end

    GH_API ==>|"HTTP GET (RFC 5988 Pagination)"| CLIENT
    CLIENT ==>|"Atomic Batch Load"| TX
    TX --> RAW_REPOS
    TX --> RAW_COMMITS
    TX --> RAW_ISSUES
    TX --> RAW_PULLS
    
    RAW_REPOS & RAW_COMMITS & RAW_ISSUES & RAW_PULLS ==>|"dbt run"| STG_VIEWS
    STG_VIEWS --> TESTS
    TESTS --> MARTS
    
    MARTS ==>|"Cached Query (@st.cache_data ttl=300)"| ENGINE
    ENGINE --> STREAMLIT
    STREAMLIT --> P1 & P2 & P3 & P4
```

---

## 🛠️ Tech Stack & Tooling

| Tool / Component | Category | Purpose in GitPulse |
| :--- | :--- | :--- |
| **Python 3.10+** | Language | Core runtime for extractors, loaders, pipeline orchestration, and dashboard. |
| **PostgreSQL 16** | Storage / Warehousing | Stores raw JSONB payloads with GIN indexing and serves analytical dbt marts. |
| **dbt Core (v1.13)** | Data Modeling | Modular SQL transformation engine managing view-to-table pipelines with automated tests. |
| **Streamlit (v1.65)** | Visualization | Responsive, interactive dark-theme web dashboard with live filtering and caching. |
| **Plotly Express** | Visual Analytics | Generates interactive heatmaps, line charts, histograms, and donut distributions. |
| **SQLAlchemy (v2.0)** | Database ORM / Pool | Manages connection pooling, atomic transaction lifecycles, and rollback safety. |
| **Tenacity** | Fault Tolerance | Industrial-grade retry decorator handling 429 and rate-limited 403 API responses. |
| **Docker** | Containerization | Provides an isolated, reproducible PostgreSQL 16 containerized environment. |

---

## 🚀 Quickstart Guide

Follow these 8 numbered steps to run GitPulse locally from scratch:

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/gitpulse.git
cd gitpulse
```

### 2. Configure Environment Variables
Create or verify `.env` in the project root:
```bash
GITHUB_TOKEN=ghp_your_personal_access_token_here
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/gitpulse
```

### 3. Spin Up PostgreSQL via Docker
Start PostgreSQL 16 in a container named `gitpulse-pg` on port 5432:
```bash
docker run -d --name gitpulse-pg -p 5432:5432 \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=gitpulse \
  postgres:16
```

### 4. Create and Activate Virtual Environment
```bash
# Windows:
python -m venv venv
venv\Scripts\activate

# macOS / Linux:
python3 -m venv venv
source venv/bin/activate
```

### 5. Install Dependencies
```bash
pip install -r requirements.txt
```

### 6. Verify Extraction Client & Staging Loader
Run the integration staging test against `apache/airflow`:
```bash
python test_pipeline.py
```

### 7. Run End-to-End Orchestrator Pipeline
Extract 10 production repositories, ingest raw JSONB, and trigger dbt transformations:
```bash
python run_pipeline.py
```

### 8. Launch the Streamlit Analytics Dashboard
```bash
streamlit run dashboard/app.py
```
Open **[http://localhost:8501](http://localhost:8501)** in your browser. *(dbt docs available at `http://localhost:8080`)*.

---

## 🧠 Architectural Design Decisions

*A portfolio perspective on why GitPulse is built this way:*

### 1. Why ELT over ETL?
> "In early data engineering architectures, transformations were executed in-flight before ingestion (ETL). While this saved storage space a decade ago, it destroys raw data fidelity: if a upstream schema shifts or business requirements evolve, the raw context is permanently lost. In GitPulse, I adopted an **ELT (Extract-Load-Transform)** pattern. Raw JSON responses from GitHub are loaded directly into PostgreSQL first, preserving 100% of the upstream context. Transformations and schema evolutions are then modeled inside version-controlled dbt models downstream. If a new metric is needed (e.g., commit message length or time-to-first-review), we simply rebuild the dbt models without ever needing to re-extract gigabytes of historical data from GitHub."

### 2. Why a JSONB Raw Staging Layer?
> "The GitHub REST API is polymorphic and frequently adds new fields (e.g., multimodal comments, draft statuses, co-authored commits). Mapping GitHub entities directly into dozens of rigid relational columns at the extraction phase causes brittle schema drift bugs. By choosing PostgreSQL's **`JSONB`** datatype coupled with **GIN indexes**, GitPulse captures the full raw payload flexibly while maintaining lightning-fast key-path lookup speeds (`payload->'author'->>'login'`). Extracted unique keys (`sha`, `number`, `full_name`) are isolated into dedicated columns with `UNIQUE` constraints to enable high-throughput upserts (`ON CONFLICT DO UPDATE SET payload = EXCLUDED.payload`)."

### 3. Why Incremental State Watermarking & ETags?
> "Full repository re-extractions waste compute and exhaust API quotas in minutes. GitPulse implements an **incremental state watermarking mechanism** (`data/state.json`). Every successful pipeline execution updates the latest UTC watermark per entity (`commits`, `issues`, `pulls`). Subsequent runs query GitHub using the `since` query parameter. This ensures the ingestion volume scales with *daily delta activity* rather than *cumulative historical volume*, keeping extraction runtimes under minutes."

### 4. Enterprise Rate-Limit & Backoff Strategy
> "GitHub's API enforces strict rate limits (60 requests/hr unauthenticated; 5,000 requests/hr authenticated) alongside secondary abuse limits. GitPulse implements a **dual-tiered defense strategy**:
> 1. **Proactive Quota Interception**: After *every single HTTP request*, the client reads `X-RateLimit-Remaining`. If remaining requests drop below 50, execution sleeps until `X-RateLimit-Reset + 5.0s`.
> 2. **Reactive Tenacity Retries**: Transient network dropouts, 429 Too Many Requests, and rate-limited 403 responses trigger an exponential backoff with jitter (`wait_exponential(min=4, max=120)`) for up to 5 attempts, ensuring automated recovery without failing the batch."

---

## 📸 Dashboard Gallery

### 1. Repository Overview
*KPI cards, daily commit velocity curve, and tracked repository metadata:*
![Repository Overview](docs/images/page1_repository_overview.png)

### 2. Developer Activity
*Top contributors leaderboard and 24x7 commit intensity schedule heatmap:*
![Developer Activity](docs/images/page2_developer_activity.png)

### 3. Pull Request Health
*Merge duration histogram, merge success rates, and weekly PR inflow vs outflow:*
![Pull Request Health](docs/images/page3_pull_request_health.png)

### 4. Issue Analytics & Backlog
*Issue resolution duration metrics, open/closed ratio, and recent backlog sample:*
![Issue Analytics](docs/images/page4_issue_analytics.png)

---

## 🔮 Future Roadmap & Production Scaling

- [ ] **Apache Airflow Migration**: Replace the standalone `run_pipeline.py` script with an Airflow DAG featuring `GitHubExtractOperator` and `DbtDag` task groups.
- [ ] **Data Warehouse Integration**: Support Snowflake and Google BigQuery destinations alongside PostgreSQL via dbt adapter switching.
- [ ] **Real-Time Webhooks via Apache Kafka**: Transition from polling to an event-driven ingestion model using GitHub Webhooks streamed into Kafka topics.
- [ ] **DORA Metrics Integration**: Compute Deployment Frequency, Lead Time for Changes, Change Failure Rate, and Time to Restore Service directly in dbt marts.

---

## 💼 Resume Line

> **GitPulse**: Engineered an end-to-end ELT engineering intelligence platform extracting GitHub REST data for 10+ open-source repos into PostgreSQL JSONB with GIN indexing, modeled 7 dbt staging and mart tables with 20 automated tests, and delivered an interactive Streamlit analytics dashboard tracking developer velocity and DORA-aligned cycle metrics.
