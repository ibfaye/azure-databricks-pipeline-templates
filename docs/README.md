# Azure Databricks Pipeline Templates — Mastery Curriculum

> **From zero to production-grade data platform engineer.**  
> A five-module, hands-on curriculum built directly against the code in this repository.  
> Every command, every config value, every code example is real — nothing is hypothetical.

---

## 📋 Curriculum Overview

| Module | Focus | Lines | Est. Time |
|--------|-------|-------|-----------|
| [**01** — Foundation](01-foundation-infrastructure-identity.md) | Azure infrastructure, IAM, Key Vault, Terraform deployment | 526 | 3–4 hrs |
| [**02** — Storage Architecture](02-storage-architecture-data-modeling.md) | ADLS Gen2, Medallion pattern, Delta Lake internals, optimization | 581 | 4–6 hrs |
| [**03** — Compute & Core Engine](03-compute-core-engine.md) | Spark architecture, cluster topology, PySpark optimization, DBR selection | 606 | 5–7 hrs |
| [**04** — Deconstructing the Templates](04-deconstructing-templates.md) | Full codebase analysis, idempotency, error handling, dbt integration | 775 | 6–8 hrs |
| [**05** — Orchestration, Governance & DataOps](05-orchestration-governance-dataops.md) | Workflows DAG, Unity Catalog RBAC, CI/CD, observability | 692 | 5–7 hrs |
| **Total** | **3,180 lines — ~140 KB of instruction** | | **23–32 hrs** |

---

## 🔗 Learning Roadmap

```
Module 1: Foundation ──────────────────────────────────────────────┐
  "I can provision the entire Azure stack from scratch."           │
       │                                                            │
       ▼                                                            │
Module 2: Storage Architecture ────────────────────────────────────┤
  "I understand every container, every abfss:// URI,               │
   and the Medallion contract."                                    │
       │                                                            │
       ▼                                                            │
Module 3: Compute & Core Engine ───────────────────────────────────┤
  "I know what Spark is doing under the hood,                      │
   and I can read the DAG in the Spark UI."                        │
       │                                                            │
       ▼                                                            │
Module 4: Deconstructing the Templates ────────────────────────────┤
  "I can trace any parameter through the codebase,                 │
   extend any module, and debug any failure."                      │
       │                                                            │
       ▼                                                            │
Module 5: Orchestration, Governance & DataOps ─────────────────────┘
  "I can deploy, monitor, secure, and automate
   the entire platform in production."
```

**Dependency graph — each module builds on the previous:**

```
  ┌──────────┐
  │ Module 1 │  Foundation (infrastructure must exist before anything runs)
  └────┬─────┘
       │
  ┌────▼─────┐
  │ Module 2 │  Storage (data structures live on the infrastructure)
  └────┬─────┘
       │
  ┌────▼─────┐
  │ Module 3 │  Compute (pipelines run on clusters that access storage)
  └────┬─────┘
       │
  ┌────▼─────┐
  │ Module 4 │  Code (understanding what the pipelines actually do)
  └────┬─────┘
       │
  ┌────▼─────┐
  │ Module 5 │  Production (wrapping everything into a governed system)
  └──────────┘
```

---

## 🎯 Who This Is For

**You** if you are:

- A software developer or systems engineer with no cloud data platform experience
- Comfortable with Python and the command line
- Familiar with basic git workflows
- Have access to an Azure subscription (or Azure free credits)
- Want to go from zero to deploying production data pipelines in one structured path

**Not you** if you:

- Already run Databricks workflows in production daily (this will be review)
- Are looking for a conceptual overview without hands-on execution
- Need a Databricks certification study guide (this is broader and deeper)

---

## 🏗️ Module Structure

Every module follows the same four-section format:

| Section | Purpose |
|---------|---------|
| **1. Learning Objectives** | What conceptual + practical skills you'll master |
| **2. Theoretical Foundations** | The design patterns, architectural constraints, and system thinking — grounded in actual repo code |
| **3. Hands-on Execution** | Step-by-step technical implementation with exact commands and code |
| **4. Validation & Troubleshooting** | How to verify success, plus common failure states with root cause analysis and fixes |

---

## 🛠️ Prerequisites

Before starting Module 1, have these ready:

- [ ] Azure subscription (free tier works for Modules 1–3; Modules 4–5 need a few dollars for compute)
- [ ] Azure CLI installed and authenticated (`az login`)
- [ ] Terraform ≥ 1.5.0 installed (`terraform version`)
- [ ] Databricks CLI installed (`databricks --version`) — optional for Module 1, required for Module 5
- [ ] GitHub CLI installed (`gh auth status`) — for CI/CD in Module 5
- [ ] Python 3.10+ with pip (`pip install dbt-databricks` — needed by Module 4)

---

## 📂 How the Modules Map to the Repository

```
azure-databricks-pipeline-templates/
│
├── terraform/                         ← Module 1 (everything here)
│   ├── providers.tf                   ← Module 1 §2.4 (Key Vault), §2.2 (provider config)
│   ├── main.tf                        ← Module 5 §2.2 (workflow as Terraform resource)
│   ├── variables.tf                   ← Module 1 §3.4 (terraform.tfvars)
│   ├── outputs.tf                     ← Module 1 §4.4 (sensitive outputs)
│   └── modules/
│       ├── azure-resources/           ← Module 1 §2.1-2.2 (VNet, subnets, ADLS, KV)
│       ├── databricks-workspace/      ← Module 1 §2.3 (SP, UC, grants)
│       └── databricks-cluster/        ← Module 3 §2.4 (cluster topologies)
│
├── pipelines/
│   ├── src/
│   │   ├── config.py                  ← Module 4 §2.1 (configuration-driven architecture)
│   │   ├── readers.py                 ← Module 4 §2.2 (ingestion contract)
│   │   ├── transformers.py            ← Module 4 §2.3 (idempotency), §2.4 (error handling)
│   │   └── writers.py                 ← Module 2 §2.5 (UC tables), Module 4 §2.5 (merge)
│   ├── notebooks/
│   │   ├── bronze_ingestion.py        ← Module 2 §2.2 (medallion trace), Module 4 §2.3
│   │   ├── silver_transformation.py   ← Module 2 §2.2, Module 4 §2.3
│   │   ├── gold_aggregation.py        ← Module 2 §2.2, Module 3 §3.6 (Photon)
│   │   ├── data_quality.py            ← Module 5 §2.5 (observability)
│   │   ├── dbt_tests.py               ← Module 4 §2.5 (dbt integration)
│   │   └── incremental_ingest.py      ← Module 4 §2.3 (availableNow trigger)
│   └── workflows/
│       └── medallion_pipeline.yml     ← Module 5 §2.1 (DAG orchestration)
│
├── dbt/
│   ├── dbt_project.yml                ← Module 4 §2.5 (tag-based catalog routing)
│   ├── packages.yml                   ← Module 4 §4.2 (semver gotchas)
│   ├── macros/utils.sql               ← Module 4 §2.5 (generate_schema_name, mask_pii)
│   └── models/
│       ├── bronze/                    ← Module 4 §2.5 (staging views)
│       │   └── schema.yml             ← Module 4 §2.5 (source definitions, tests)
│       ├── silver/                    ← Module 4 §3.7 (new model pattern)
│       └── gold/                      ← Module 4 §2.5
│
├── .github/workflows/
│   ├── terraform-validate.yml         ← Module 5 §2.4 (infrastructure CI)
│   └── dbt-ci.yml                     ← Module 5 §2.4 (dbt CI)
│
└── docs/
    ├── architecture.md                ← High-level system design
    ├── getting-started.md             ← Quick-start guide
    ├── use-cases/                     ← Example use case documentation
    └── 01–05 *.md                     ← THIS CURRICULUM
```

---

## 🚀 Quick Start

If you want to jump straight to running code, start here:

1. **Read** [`getting-started.md`](getting-started.md) for a 15-minute overview
2. **Read** [`architecture.md`](architecture.md) for the system design
3. **Begin Module 1** — provision the infrastructure. Everything else depends on this.

Expected pace: ~1 module per day for a dedicated learner, or ~1 module per week alongside other work.

---

## 📊 Completion Milestones

| Module | Milestone | Verifiable By |
|--------|-----------|---------------|
| 1 | Zero-diff `terraform plan` | `terraform plan` → "No changes" |
| 2 | Record traced through all 3 layers | SQL queries showing the same `transaction_id` in Bronze, Silver (masked), Gold (aggregated) |
| 3 | Native function is 5×+ faster than equivalent UDF | Timed benchmark in notebook |
| 4 | New dbt model routes to correct UC catalog | `dbt compile` output shows correct `catalog.schema` prefix |
| 5 | `git push` → CI passes → pipeline runs → DQ dashboard shows results | Fully automated end-to-end |

---

## 🔗 Related Resources

- **Repository:** [github.com/ibfaye/azure-databricks-pipeline-templates](https://github.com/ibfaye/azure-databricks-pipeline-templates)
- **Databricks Docs:** [docs.databricks.com](https://docs.databricks.com/)
- **Delta Lake Docs:** [delta.io](https://delta.io/)
- **dbt Docs:** [docs.getdbt.com](https://docs.getdbt.com/)
- **Terraform Azure Provider:** [registry.terraform.io/providers/hashicorp/azurerm](https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs)
- **Terraform Databricks Provider:** [registry.terraform.io/providers/databricks/databricks](https://registry.terraform.io/providers/databricks/databricks/latest/docs)
