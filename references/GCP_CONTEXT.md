# GCP Context

This repository uses a dedicated Google Cloud project for Apps Script logging and diagnostics.

## BIBIV Onboarding

| Setting | Value |
|---|---|
| Project ID | `bibiv-onboarding-app` |
| Project number | `205037587961` |
| Log reader service account | `codex-logs-reader@bibiv-onboarding-app.iam.gserviceaccount.com` |
| Auth model | Keyless service account impersonation |
| Intended role on project | `roles/logging.viewer` |
| Intended impersonator | `hypnotype@hypnotype.com` with `roles/iam.serviceAccountTokenCreator` on the service account |

Do not create or commit service account keys for this project.

## Local Usage

Load the project context from the repository root:

```bash
source scripts/use-gcp-bibiv-onboarding.sh
```

Read recent Apps Script errors:

```bash
bibiv_logs_errors 100
```

Read document generator worker logs:

```bash
bibiv_logs_docgen 100
```

Read logs for any Apps Script function:

```bash
bibiv_logs_function processNextQueuedDocGenerationJob 100
```

Search Apps Script log messages:

```bash
bibiv_logs_search DOCGEN_ 100
```

Run arbitrary `gcloud` commands in the BIBIV project with the log-reader service account:

```bash
bibiv_gcloud logging read 'resource.type="app_script_function"' --limit=20 --format=json
```

Equivalent explicit command:

```bash
gcloud logging read \
  'resource.type="app_script_function" AND severity>=ERROR' \
  --project=bibiv-onboarding-app \
  --impersonate-service-account=codex-logs-reader@bibiv-onboarding-app.iam.gserviceaccount.com \
  --limit=100 \
  --format=json
```

## Notes For Agents

When working in this repository, do not rely on the globally active `gcloud` project. The user may switch between unrelated projects. For GCP diagnostics related to this app, use the context above or source `scripts/use-gcp-bibiv-onboarding.sh`.

The service account is intentionally read-only for logs. If a task needs writes, deployment, IAM changes, Drive/AppSheet access, or Apps Script edits, use the appropriate existing tooling and ask before expanding IAM scope.
