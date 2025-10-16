# 🏗️ Modern Data Lakehouse Platform

A production-ready data lakehouse implementing medallion architecture (Bronze → Silver → Gold) with Apache Airflow orchestration and comprehensive monitoring.

---

## Overview

This project demonstrates a complete data engineering pipeline that processes NYC taxi trip data through multiple quality stages, stores it in a data lake (MinIO), loads it into a data warehouse (PostgreSQL), and provides full observability through Prometheus and Grafana.

---

## Architecture
```
┌────────────────────────────────────────────────────────────────┐
│                   DATA LAKEHOUSE PLATFORM                       │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  DATA PROCESSING LAYERS                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌─────────┐ │
│  │  BRONZE  │───▶│  SILVER  │───▶│   GOLD   │───▶│   DWH   │ │
│  │   Raw    │    │ Cleaned  │    │Aggregated│    │Postgres │ │
│  │  Data    │    │Validated │    │ Business │    │         │ │
│  └──────────┘    └──────────┘    └──────────┘    └─────────┘ │
│       │               │                │               │       │
│       └───────────────┴────────────────┴───────────────┘       │
│                           │                                     │
│                      ┌─────────┐                               │
│                      │  MinIO  │                               │
│                      │ Storage │                               │
│                      └─────────┘                               │
│                                                                 │
│  ORCHESTRATION                                                  │
│  ┌────────────────────────────────────────────┐               │
│  │        Apache Airflow                       │               │
│  │  • Workflow Scheduling • Monitoring         │               │
│  └────────────────────────────────────────────┘               │
│                                                                 │
│  VISUALIZATION                                                  │
│  ┌─────────────┐              ┌─────────────┐                 │
│  │  Metabase   │              │   Grafana   │                 │
│  │ BI Platform │              │  Dashboards │                 │
│  └─────────────┘              └─────────────┘                 │
│                                                                 │
│  MONITORING                                                     │
│  ┌────────────────────────────────────────────┐               │
│  │      Prometheus + Exporters                 │               │
│  │  • System • Database • Network              │               │
│  └────────────────────────────────────────────┘               │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
```

**Components:**
- **Bronze Layer**: Raw data ingestion to MinIO
- **Silver Layer**: Data cleaning and validation
- **Gold Layer**: Business aggregations and metrics
- **Data Warehouse**: PostgreSQL for analytics queries
- **Orchestration**: Apache Airflow for pipeline automation
- **Monitoring**: Prometheus + Grafana for observability
- **Analytics**: Metabase for business intelligence

---


## Tech Stack

Apache Airflow • MinIO • PostgreSQL • Prometheus • Grafana • Metabase • Docker

---

## License

MIT License - see [LICENSE](LICENSE) file for details.
