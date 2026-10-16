"""Publication exclusions must protect local credentials without hiding source."""

import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def ignored(path: str) -> bool:
    result = subprocess.run(
        ["git", "check-ignore", "--no-index", "--stdin"],
        input=path + "\n", text=True, capture_output=True, cwd=ROOT, check=False,
    )
    assert result.returncode in (0, 1), result.stderr
    return result.returncode == 0


@pytest.mark.parametrize("path", [
    ".env", ".env.production", "nested/.env", "deployment/production.env",
    "deployment/production.env.bak", "nested/secrets/token.json",
    ".aws/credentials", ".ssh/id_ed25519", "deployment/server.pem",
    "deployment/signing.key", "credentials.json", "credentials.ini",
    ".streamlit/secrets.toml", ".boto", ".s3cfg", ".ssh/id_ecdsa", "wallet.json",
    "wallet1.json", "wallet-backup.txt", "UTC--2026-01-01--account", "keypair",
    "nested/wallet-mainnet.json", "nested/id.json", "nested/id_test.json",
    "nested/solana-keypair.json", "nested/private_key.json",
    "nested/secret_key.json", "nested/seed_phrase.txt", "nested/mnemonic.txt",
    "nested/keystore/account.json", "wallets/account.json",
    "output/inspect_labels.txt", "output/dataset_summary.json",
    "wallet_leverage_decomposition/data/reservoir/snapshot.parquet",
    "wallet_leverage_decomposition/Effective Leverage Plots.ipynb",
    "wallet_leverage_decomposition/Effective Leverage Plots.html",
    "wallet_leverage_decomposition/.virtual_documents/analysis.ipynb",
    "logs/ingestion.log", "ingestion.log.1", "backups/database.sql",
    "database.dump", "database.sql.gz", "pgdata/base/123",
    "deployment/terraform.tfstate", "deployment/terraform.tfstate.backup",
    "deployment/production.tfvars", ".agents/skills/third-party/SKILL.md",
    ".claude/settings.json", ".pi/sessions/session.jsonl", "skills-lock.json",
])
def test_local_credentials_and_generated_artifacts_are_ignored(path: str) -> None:
    assert ignored(path), f"Sensitive/local path is publishable: {path}"


@pytest.mark.parametrize("path", [
    ".env.example", "nested/.env.example", "deployment/production.env.example",
    "deployment/production.tfvars.example", ".gitleaks.toml", "SECURITY.md",
    ".github/workflows/secret-scan.yml", "README.md", "AGENTS.md",
    "pyproject.toml", "run_ingestion.sh", "sql/inspect_labels.sql",
    "sql/restrict_to_majors.sql", "src/hydromancer_eda/client/ws.py",
    "wallet_leverage_decomposition/effective_leverage.py",
    "wallet_leverage_decomposition/test_leverage_decomposition.py",
    "tests/test_publication_safety.py",
])
def test_source_and_placeholder_templates_remain_publishable(path: str) -> None:
    assert not ignored(path), f"Publishable source is accidentally hidden: {path}"


def test_no_already_tracked_files_are_now_ignored() -> None:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--ignored", "--exclude-standard"],
        text=True, capture_output=True, cwd=ROOT, check=True,
    )
    assert not result.stdout.strip(), "Tracked files need explicit review: " + result.stdout
