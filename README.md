<!-- GitAds-Verify: 5MHXVOAVLQUR1F1OTMME2QV4876LJJO7 -->

# MLOps Engineering Roadmap 2026

## GitAds Sponsored

[![Sponsored by GitAds](https://gitads.dev/v1/ad-serve?source=atryx/mlops-engineering-roadmap-2026@github)](https://gitads.dev/v1/ad-track?source=atryx/mlops-engineering-roadmap-2026@github)

A structured path to becoming a production MLOps engineer in 2026. Covers everything from experiment tracking to GPU infrastructure — the engineering that turns ML notebooks into reliable production systems.

> **Star this repo** ⭐ — MLOps tooling changes fast. We keep this updated.

---

## Table of Contents

- [What is MLOps?](#what-is-mlops)
- [Phase 1: Foundations](#phase-1-foundations)
- [Phase 2: Experiment Tracking & Versioning](#phase-2-experiment-tracking--versioning)
- [Phase 3: Data Pipelines & Feature Stores](#phase-3-data-pipelines--feature-stores)
- [Phase 4: Model Training Infrastructure](#phase-4-model-training-infrastructure)
- [Phase 5: Model Serving & Inference](#phase-5-model-serving--inference)
- [Phase 6: CI/CD for ML](#phase-6-cicd-for-ml)
- [Phase 7: Monitoring & Observability](#phase-7-monitoring--observability)
- [Phase 8: LLMOps](#phase-8-llmops)
- [Phase 9: GPU Infrastructure & Cost](#phase-9-gpu-infrastructure--cost)
- [Phase 10: Platform Engineering for ML](#phase-10-platform-engineering-for-ml)
- [MLOps Maturity Levels](#mlops-maturity-levels)
- [The 2026 MLOps Stack](#the-2026-mlops-stack)
- [Project Ideas by Level](#project-ideas-by-level)
- [Related Repos](#related-repos)

---

## What is MLOps?

MLOps = **DevOps for Machine Learning**. It bridges the gap between data science experimentation and production systems.

```
Data Scientist (notebook) → MLOps Engineer (production) → End Users
```

| Data Science | MLOps Engineering |
|-------------|-------------------|
| Jupyter notebooks | Reproducible pipelines |
| Manual experiments | Automated training + eval |
| "Works on my machine" | Containerized, tested, versioned |
| One-off model exports | Automated model serving + monitoring |
| "The model is done" | "The model is deployed, monitored, and retrained" |

---

## Phase 1: Foundations

**Time:** 4-6 weeks

### Prerequisites

- [ ] **Python** — intermediate level, virtual environments, packaging
- [ ] **Docker** — Dockerfiles, Compose, registries
- [ ] **Git** — branching, PRs, tags
- [ ] **Linux** — CLI, SSH, process management
- [ ] **Cloud basics** — one of AWS/Azure/GCP (compute, storage, IAM)
- [ ] **SQL** — querying, joins, aggregations
- [ ] **ML basics** — training, evaluation, overfitting, train/test split (you don't need to be an ML expert)

> 📘 See also: [devops-learning-path](https://github.com/atryx/devops-learning-path) | [docker-cheatsheet](https://github.com/atryx/docker-cheatsheet)

### ML Lifecycle

```
Define Problem → Collect Data → Process Data → Train Model → Evaluate → Deploy → Monitor → Retrain
                      ↑                                                              |
                      └──────────────────────────────────────────────────────────────┘
```

Every phase needs tooling. That's your job.

---

## Phase 2: Experiment Tracking & Versioning

**Time:** 3-4 weeks

### Experiment Tracking ⭐

Without tracking, ML is "I tried some stuff and this seemed to work."

| Tool | Type | Best For |
|------|------|----------|
| **MLflow** ⭐ | OSS, self-hosted | Default choice, industry standard |
| **Weights & Biases** ⭐ | SaaS | Best UX, rich visualizations |
| **Neptune** | SaaS | Enterprise, metadata management |
| **ClearML** | OSS/SaaS | Full pipeline, free tier |
| **Comet** | SaaS | Experiment comparison |

### What to Track

```python
import mlflow

mlflow.set_experiment("fraud-detection-v2")

with mlflow.start_run():
    # Log parameters
    mlflow.log_param("model_type", "xgboost")
    mlflow.log_param("learning_rate", 0.01)
    mlflow.log_param("max_depth", 6)
    mlflow.log_param("n_estimators", 500)

    # Train model
    model = train(params)

    # Log metrics
    mlflow.log_metric("accuracy", 0.94)
    mlflow.log_metric("f1_score", 0.87)
    mlflow.log_metric("auc_roc", 0.96)

    # Log artifacts
    mlflow.log_artifact("confusion_matrix.png")
    mlflow.sklearn.log_model(model, "model")

    # Log dataset info
    mlflow.log_param("dataset_version", "v2.3")
    mlflow.log_param("training_samples", 150_000)
```

### Data Versioning

| Tool | What It Does |
|------|-------------|
| **DVC** ⭐ | Git for data — version large files, track pipelines |
| **LakeFS** | Git-like operations on data lakes |
| **Delta Lake** | Versioned Parquet tables (Spark ecosystem) |
| **Pachyderm** | Data versioning + pipelines |

### Model Registry

- [ ] **MLflow Model Registry** ⭐ — stage models through None → Staging → Production → Archived
- [ ] Version models with metadata (metrics, dataset version, training config)
- [ ] Approval workflows before production promotion
- [ ] Lineage tracking (which data + code → which model)

---

## Phase 3: Data Pipelines & Feature Stores

**Time:** 4-6 weeks

### Data Pipeline Orchestration ⭐

| Tool | Best For | Complexity |
|------|----------|-----------|
| **Airflow** ⭐ | Industry standard, any workflow | High |
| **Prefect** | Modern Airflow alternative | Medium |
| **Dagster** | Data-aware orchestration | Medium |
| **Mage** | Low-code data pipelines | Low |
| **Temporal** | Complex stateful workflows | High |

### Airflow DAG Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract_data(): ...
def transform_data(): ...
def train_model(): ...
def evaluate_model(): ...
def deploy_if_better(): ...

with DAG(
    "ml_training_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ml", "training"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=extract_data)
    transform = PythonOperator(task_id="transform", python_callable=transform_data)
    train = PythonOperator(task_id="train", python_callable=train_model)
    evaluate = PythonOperator(task_id="evaluate", python_callable=evaluate_model)
    deploy = PythonOperator(task_id="deploy", python_callable=deploy_if_better)

    extract >> transform >> train >> evaluate >> deploy
```

### Feature Stores ⭐

Feature stores solve: "How do I use the same features in training AND serving without skew?"

| Tool | Type | Best For |
|------|------|----------|
| **Feast** ⭐ | OSS | Default choice, cloud-agnostic |
| **Tecton** | SaaS | Enterprise, real-time features |
| **Hopsworks** | OSS/SaaS | Full ML platform |
| **SageMaker Feature Store** | AWS | AWS-native |
| **Vertex AI Feature Store** | GCP | GCP-native |

### Key Concepts

- [ ] **Online store** — low-latency serving (Redis, DynamoDB) for real-time inference
- [ ] **Offline store** — batch access (S3, BigQuery) for training
- [ ] **Feature engineering** — transforming raw data into model inputs
- [ ] **Training-serving skew** — when features differ between training and production
- [ ] **Point-in-time correctness** — using features as they were at prediction time (no data leakage)

---

## Phase 4: Model Training Infrastructure

**Time:** 4-6 weeks

### Training Compute

| Option | When to Use | Cost |
|--------|-------------|------|
| **Local GPU** | Prototyping, small models | $ (upfront) |
| **Cloud VMs** (EC2, GCE) | Medium jobs, custom setup | $$ |
| **Managed** (SageMaker, Vertex) | Production training, auto-scaling | $$$ |
| **Serverless GPU** (Modal, RunPod) | Burst training, variable demand | $$ |
| **On-prem cluster** | Constant high demand, compliance | $$$$ (but cheaper per hour) |

### Distributed Training

When one GPU isn't enough:

- [ ] **Data parallelism** — same model on multiple GPUs, split the data
- [ ] **Model parallelism** — split the model across GPUs (for huge models)
- [ ] **Pipeline parallelism** — different layers on different GPUs
- [ ] **DeepSpeed** / **FSDP** — frameworks for efficient distributed training
- [ ] **Horovod** — distributed training framework (Uber)

### Hyperparameter Optimization

| Tool | Method |
|------|--------|
| **Optuna** ⭐ | Bayesian optimization, pruning |
| **Ray Tune** | Distributed HPO, multiple algorithms |
| **Weights & Biases Sweeps** | Grid, random, Bayesian |
| **SageMaker HPO** | AWS-native |

### Reproducibility Checklist

- [ ] Pin all dependencies (`requirements.txt` or `uv.lock`)
- [ ] Version training data (DVC)
- [ ] Log all hyperparameters (MLflow/W&B)
- [ ] Set random seeds
- [ ] Containerize training jobs (Docker)
- [ ] Track hardware/environment info

---

## Phase 5: Model Serving & Inference

**Time:** 4-6 weeks

### Serving Approaches

| Approach | Latency | Use Case |
|----------|---------|----------|
| **Real-time API** | < 100ms | Recommendations, fraud detection |
| **Batch inference** | Hours | Scoring datasets, reports |
| **Streaming** | Near-real-time | Event processing, anomaly detection |
| **Edge** | < 10ms | Mobile, IoT, embedded |

### Serving Frameworks ⭐

| Tool | Best For |
|------|----------|
| **TorchServe** | PyTorch models |
| **TensorFlow Serving** | TF models |
| **Triton Inference Server** ⭐ | Multi-framework, GPU optimization, batching |
| **vLLM** ⭐ | LLM serving (high throughput) |
| **TGI** (Text Generation Inference) | LLM serving (Hugging Face) |
| **BentoML** | Multi-framework, easy packaging |
| **Seldon Core** | Kubernetes-native model serving |
| **KServe** | Kubernetes, serverless inference |
| **FastAPI** | Simple models, custom logic |

### LLM Serving (2026) ⭐

| Tool | What It Does |
|------|-------------|
| **vLLM** ⭐ | PagedAttention, continuous batching, highest throughput |
| **TGI** | Hugging Face models, good defaults |
| **llama.cpp** | CPU inference, quantized models |
| **Ollama** | Local model running, developer-friendly |
| **SGLang** | Efficient structured generation |

### Model Optimization

- [ ] **Quantization** — FP32 → FP16 → INT8 → INT4 (smaller, faster, slight accuracy loss)
- [ ] **Pruning** — remove unimportant weights
- [ ] **Distillation** — train small model to mimic large model
- [ ] **ONNX Runtime** — cross-platform inference optimization
- [ ] **TensorRT** — NVIDIA GPU optimization
- [ ] **Batching** — dynamic batching for throughput

---

## Phase 6: CI/CD for ML

**Time:** 3-4 weeks

### ML-Specific CI/CD Challenges

Regular software: code → test → deploy
ML: code + data + model + config → test everything → deploy model + serving infra

### ML Pipeline

```yaml
# .github/workflows/ml-pipeline.yml
name: ML Pipeline
on:
  push:
    paths:
      - 'src/**'
      - 'data/**'
      - 'configs/**'

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install -r requirements.txt
      - run: pytest tests/unit/
      - run: pytest tests/data/  # Data validation tests

  train:
    needs: test
    runs-on: [self-hosted, gpu]
    steps:
      - uses: actions/checkout@v4
      - run: python train.py --config configs/prod.yaml
      - run: python evaluate.py --threshold 0.90
      - uses: actions/upload-artifact@v4
        with:
          name: model
          path: models/

  deploy:
    needs: train
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - run: echo "Register model and deploy"
```

### What to Test in ML

| Test Type | What | Example |
|-----------|------|---------|
| **Data validation** | Schema, distributions, nulls | Great Expectations, Pandera |
| **Unit tests** | Feature engineering functions | pytest |
| **Model validation** | Metrics meet threshold | Custom eval scripts |
| **Integration** | Full pipeline runs | End-to-end test job |
| **Shadow testing** | New model vs production (same inputs) | A/B or shadow deployment |
| **Bias/fairness** | Model doesn't discriminate | Fairlearn, AIF360 |

---

## Phase 7: Monitoring & Observability

**Time:** 4-6 weeks

### ML Monitoring ⭐

Regular monitoring: latency, errors, throughput.
ML monitoring: **all of that PLUS model performance, data quality, and drift**.

### What to Monitor

| Category | Metrics | Tools |
|----------|---------|-------|
| **System** | Latency, throughput, errors, GPU utilization | Prometheus, Grafana |
| **Data drift** | Feature distribution changes | Evidently ⭐, NannyML, Whylogs |
| **Model performance** | Accuracy, F1, AUC over time | Evidently, NannyML, custom |
| **Prediction drift** | Output distribution changes | Evidently |
| **Data quality** | Missing values, schema violations | Great Expectations |

### Data & Model Drift ⭐

```
Training data (Jan 2026) → Model trained → Deployed
                                              ↓
Production data (Jun 2026) ←── Distributions changed!
                                              ↓
Model accuracy drops ← "drift"
```

**Types of drift:**
- [ ] **Data drift** — input feature distributions change
- [ ] **Concept drift** — relationship between features and target changes
- [ ] **Prediction drift** — model output distributions change
- [ ] **Label drift** — ground truth distribution changes

### Alerting Rules

```yaml
# Example: Prometheus alert for model serving
groups:
  - name: ml-model-alerts
    rules:
      - alert: HighInferenceLatency
        expr: histogram_quantile(0.95, rate(model_inference_duration_seconds_bucket[5m])) > 0.5
        for: 5m
        labels:
          severity: warning

      - alert: PredictionDrift
        expr: model_prediction_drift_score > 0.3
        for: 15m
        labels:
          severity: critical
        annotations:
          summary: "Model predictions are drifting — consider retraining"

      - alert: HighErrorRate
        expr: rate(model_inference_errors_total[5m]) / rate(model_inference_requests_total[5m]) > 0.05
        for: 5m
        labels:
          severity: critical
```

---

## Phase 8: LLMOps

**Time:** 4-6 weeks

### LLMOps vs Traditional MLOps

| Traditional MLOps | LLMOps |
|-------------------|--------|
| Train your own model | Use API / fine-tune existing model |
| Feature engineering | Prompt engineering |
| Model accuracy metrics | LLM eval (faithfulness, relevance, safety) |
| Data pipelines | RAG pipelines |
| Model registry | Prompt registry + model router |
| Retraining on new data | Updating knowledge base, re-embedding |

### LLMOps Stack

| Concern | Tools |
|---------|-------|
| **Prompt management** | LangSmith, Promptfoo, Humanloop |
| **Eval** ⭐ | Promptfoo, Ragas, DeepEval, Braintrust |
| **Monitoring** | Langfuse ⭐, LangSmith, Helicone |
| **Gateway/routing** | LiteLLM, Portkey, custom |
| **Guardrails** | Guardrails AI, NeMo Guardrails |
| **Vector DB ops** | Index management, re-embedding pipelines |
| **Cost tracking** | Helicone, custom token tracking |

### Key LLMOps Practices

- [ ] Version control prompts alongside code
- [ ] Automated eval suites (run on every prompt change)
- [ ] A/B test model versions and prompts
- [ ] Monitor token usage and cost per user/feature
- [ ] Semantic caching for repeated queries
- [ ] Fallback chains (primary model → fallback → cached response)
- [ ] PII detection and redaction
- [ ] Rate limiting per user/tier

> 📘 See also: [ai-engineer-roadmap-2026](https://github.com/atryx/ai-engineer-roadmap-2026)

---

## Phase 9: GPU Infrastructure & Cost

**Time:** 3-4 weeks

### GPU Landscape (2026)

| GPU | VRAM | Best For | Cloud Cost (approx/hr) |
|-----|------|----------|----------------------|
| **NVIDIA A100** | 40/80GB | Training, fine-tuning | $3-4 |
| **NVIDIA H100** | 80GB | LLM training, high performance | $8-12 |
| **NVIDIA H200** | 141GB | Large model training | $12-15 |
| **NVIDIA L4** | 24GB | Inference, cost-effective | $0.8-1 |
| **NVIDIA T4** | 16GB | Light inference, budget | $0.5-0.7 |
| **AMD MI300X** | 192GB | Training alternative to H100 | Varies |

### Cost Optimization ⭐

- [ ] **Spot/preemptible instances** — 60-90% savings for training (with checkpointing)
- [ ] **Right-size GPUs** — don't use H100 for inference that runs fine on L4
- [ ] **Quantization** — INT8/INT4 models serve on smaller GPUs
- [ ] **Auto-scaling** — scale to zero when no traffic (KServe, Knative)
- [ ] **Batch inference** — process in bulk during off-peak hours
- [ ] **Reserved instances** — for steady-state inference workloads
- [ ] **Multi-tenancy** — share GPU clusters across teams

### GPU Orchestration

| Tool | What It Does |
|------|-------------|
| **Kubernetes + NVIDIA GPU Operator** | GPU scheduling in K8s |
| **Ray** ⭐ | Distributed compute (training + serving) |
| **Slurm** | HPC job scheduler (on-prem) |
| **SkyPilot** | Multi-cloud GPU orchestration |
| **Modal** | Serverless GPU (pay per second) |
| **RunPod** | On-demand GPU cloud |

---

## Phase 10: Platform Engineering for ML

**Time:** Ongoing (senior level)

### Internal ML Platform

Build self-service infrastructure so data scientists can go from notebook to production without filing tickets.

**Components:**
- [ ] **Experiment workspace** — managed notebooks with GPU access
- [ ] **Training pipeline builder** — UI or config to launch training jobs
- [ ] **Model registry** — versioned models with metadata and lineage
- [ ] **Serving infrastructure** — one-click model deployment
- [ ] **Monitoring dashboard** — drift, performance, cost per model
- [ ] **Feature store** — shared features with online/offline serving
- [ ] **Data catalog** — discover available datasets and features

### Reference Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Developer Portal                   │
│            (Backstage / Custom UI)                   │
├──────────┬──────────┬──────────┬───────────────────┤
│ Notebooks│ Training │ Serving  │ Monitoring        │
│ (JupyterHub) (Airflow)  (Triton)  (Grafana+Evidently)│
├──────────┴──────────┴──────────┴───────────────────┤
│              Feature Store (Feast)                    │
├─────────────────────────────────────────────────────┤
│          Experiment Tracking (MLflow / W&B)          │
├─────────────────────────────────────────────────────┤
│           Data Lake (S3 / GCS / Delta Lake)          │
├─────────────────────────────────────────────────────┤
│      Kubernetes + GPU Operator + Ray + Terraform     │
└─────────────────────────────────────────────────────┘
```

---

## MLOps Maturity Levels

| Level | Description | What You Have |
|-------|-------------|---------------|
| **0 — Manual** | Everything by hand | Notebooks, manual deployment, no monitoring |
| **1 — Pipeline** | Automated training pipeline | Orchestrated training, basic CI/CD, experiment tracking |
| **2 — CI/CD** | Automated training + deployment | Model registry, automated eval, staged rollouts |
| **3 — Monitoring** | Production monitoring + retraining | Drift detection, automated retraining triggers, A/B testing |
| **4 — Platform** | Self-service ML platform | Feature store, model catalog, automated everything |

Most companies are at Level 0-1. Getting to Level 2-3 is the sweet spot for most teams.

---

## The 2026 MLOps Stack

### Recommended for Most Teams

| Layer | Tool | Alternative |
|-------|------|------------|
| **Orchestration** | Airflow / Prefect | Dagster |
| **Experiment Tracking** | MLflow | Weights & Biases |
| **Data Versioning** | DVC | LakeFS |
| **Feature Store** | Feast | Tecton (enterprise) |
| **Training** | SageMaker / Vertex / Ray | Custom K8s jobs |
| **Serving (traditional ML)** | Triton / BentoML | Seldon, KServe |
| **Serving (LLMs)** | vLLM / TGI | Ollama (dev) |
| **Monitoring** | Evidently + Grafana | NannyML, WhyLabs |
| **LLM Monitoring** | Langfuse / LangSmith | Helicone |
| **Infrastructure** | Kubernetes + Terraform | ECS, Cloud Run |

---

## Project Ideas by Level

### Beginner
1. **End-to-end pipeline** — data → train → evaluate → log to MLflow
2. **Model API** — serve a scikit-learn model with FastAPI + Docker
3. **Experiment tracking** — train 10 model variants, compare in MLflow/W&B

### Intermediate
4. **Automated retraining** — Airflow DAG that retrains weekly, deploys if better
5. **Feature store** — Feast with online (Redis) + offline (Parquet) stores
6. **Drift monitoring** — Evidently dashboard detecting data and prediction drift

### Advanced
7. **LLM serving platform** — vLLM on K8s with auto-scaling, multi-model routing
8. **Full ML CI/CD** — data validation → train → eval → shadow test → canary deploy
9. **Multi-model A/B testing** — route traffic between model versions, measure business metrics

### Senior / Platform
10. **Internal ML platform** — self-service training, serving, monitoring for data science team
11. **GPU cluster management** — K8s + GPU Operator + Ray + spot instances + cost controls
12. **Automated ML pipeline generator** — config-driven pipeline creation (no code for data scientists)

---

## Related Repos

| Repo | What's Inside |
|------|---------------|
| [ai-engineer-roadmap-2026](https://github.com/atryx/ai-engineer-roadmap-2026) | AI engineer learning path |
| [langchain-vs-llamaindex](https://github.com/atryx/langchain-vs-llamaindex) | RAG framework comparison |
| [devops-learning-path](https://github.com/atryx/devops-learning-path) | DevOps from zero to production |
| [backend-developer-roadmap-2026](https://github.com/atryx/backend-developer-roadmap-2026) | Backend developer roadmap |
| [docker-cheatsheet](https://github.com/atryx/docker-cheatsheet) | Docker commands quick reference |
| [production-ready-snippets](https://github.com/atryx/production-ready-snippets) | Production config snippets |
| [prompt-engineering-cheatsheet](https://github.com/atryx/prompt-engineering-cheatsheet) | Prompt engineering techniques |
| [awesome-dev-errors](https://github.com/atryx/awesome-dev-errors) | Real error messages + fixes |

---

## Contributing

ML tooling moves fast. Know a better tool? See something outdated? PRs welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE)
