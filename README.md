# PRGuard — Automated GitHub PR Review Platform

An event-driven microservices platform that automatically reviews GitHub Pull Requests using AI and security scanning, then posts results as PR comments.

## Architecture

```
GitHub PR Event
      │
      ▼
┌─────────────────┐
│ webhook-service │  FastAPI — validates HMAC signature, publishes to Kafka
└────────┬────────┘
         │ Kafka: pr-events
    ┌────┴─────┐
    ▼           ▼
┌──────────────────┐   ┌──────────────────┐
│ ai-review-worker │   │ security-scanner │
│  (Claude Haiku)  │   │  (regex rules)   │
└────────┬─────────┘   └────────┬─────────┘
         │ ai-results            │ security-results
         └──────────┬────────────┘
                    ▼
          ┌──────────────────┐
          │  result-service  │  saves to PostgreSQL + posts PR comment
          └──────────────────┘
                    │ ai-results
                    ▼
          ┌──────────────────┐
          │    notifier      │  sends email alert for low-score PRs
          └──────────────────┘
```

## Services

| Service | Tech | Responsibility |
|---------|------|----------------|
| webhook-service | FastAPI, Kafka | Receives GitHub webhooks, validates HMAC-SHA256, publishes PR events |
| ai-review-worker | Claude Haiku API, Kafka | Consumes PR events, analyzes code diff, publishes AI review results |
| security-scanner | Regex, Kafka | Scans new code lines for secrets, SQL injection, shell injection |
| result-service | PostgreSQL, GitHub API | Saves results to DB, posts formatted comments on GitHub PR |
| notifier | Gmail SMTP, Kafka | Sends email alerts when PR score ≤ 6 or HIGH severity issues found |

## Tech Stack

- **Languages**: Python 3.11
- **API Framework**: FastAPI + Uvicorn
- **Message Queue**: Apache Kafka
- **AI**: Anthropic Claude Haiku 4.5 API
- **Database**: PostgreSQL
- **Containerization**: Docker + Docker Compose
- **Orchestration**: Kubernetes (Docker Desktop) with HPA auto-scaling
- **CI/CD**: GitHub Actions (23 unit tests)
- **Webhook Tunnel**: ngrok (fixed domain)

## Getting Started

### Prerequisites

- Docker Desktop with Kubernetes enabled
- ngrok account (for local webhook testing)
- Anthropic API key
- GitHub Personal Access Token
- Gmail App Password

### Environment Variables

Create a `.env` file in the project root:

```env
GITHUB_WEBHOOK_SECRET=your_webhook_secret
GITHUB_TOKEN=your_github_personal_access_token
ANTHROPIC_API_KEY=your_anthropic_api_key
GMAIL_USER=your_gmail@gmail.com
GMAIL_APP_PASSWORD=your_16_char_app_password
NOTIFY_EMAIL=recipient@gmail.com
```

### Run Locally

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f ai-review-worker
docker-compose logs -f notifier

# Restart a service after code changes
docker-compose restart ai-review-worker
```

### Deploy to Kubernetes

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/webhook-service.yaml
kubectl apply -f k8s/ai-review-worker.yaml
kubectl apply -f k8s/security-scanner.yaml
kubectl apply -f k8s/result-service.yaml
kubectl apply -f k8s/notifier.yaml

# Check status
kubectl get pods -n prguard
kubectl get hpa -n prguard
```

### Run Tests

```bash
cd services/webhook-service && pytest test_main.py -v
cd services/ai-review-worker && pytest test_main.py -v
cd services/security-scanner && pytest test_main.py -v
cd services/result-service && pytest test_main.py test_database.py -v
cd services/notifier && pytest test_main.py -v
```

## AI Review Output

Each PR receives an automated comment with:

- **Score**: 1–10 code quality score
- **Issues**: categorized by severity (HIGH / MEDIUM / LOW)
- **Suggestions**: actionable improvement recommendations
- **Status**: Approved or Changes Requested


## Kubernetes Setup

The AI Review Worker is configured with **Horizontal Pod Autoscaler (HPA)**:

- Min replicas: 1
- Max replicas: 5
- Scale up trigger: CPU utilization > 70%

This ensures the system handles PR review spikes automatically without manual intervention.

## CI/CD

GitHub Actions runs 23 unit tests across all 5 services on every push and pull request. Merging is blocked if any test fails.
