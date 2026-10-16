# Publishing safely

This project consumes authenticated market data. It does not need wallet private
keys, signing keypairs, or recovery phrases. Never add those to the repository.
Public wallet addresses and transaction hashes are not signing secrets, but wallet
exports can still reveal which accounts you monitor.

## What stays local

The root `.gitignore` excludes credentials, dotenv files other than example
files, common wallet keypair/seed backups, raw data, database dumps, operational
logs, cursors/checkpoints, generated reports, all notebooks and HTML exports,
Jupyter virtual documents, and local agent/editor configuration. The Python
analysis code, SQL, tests, and placeholder configuration remain publishable.

- Keep example configuration files strictly placeholder-only.
- Do not use `git add -f` to override exclusions for secrets or generated data.
- To share a notebook, create a separately reviewed sanitized example. Removing
  visible code is not enough: cell output, metadata, attachments, HTML, and widget
  state can retain tokens or private data.
- Review data-provider redistribution terms before publishing data or charts.
- Do not copy installed third-party agent skills into the public repository
  without checking their license.
- Share the Git repository, not a ZIP of the working directory. `.gitignore`
  does not remove local files from an archive made by Finder or another tool.

## Check before making the repository public

Install Gitleaks (the CI workflow pins version 8.30.1), then run:

```bash
gitleaks git . --log-opts="--all --full-history" --redact=100 --ignore-gitleaks-allow
git ls-files --cached --ignored --exclude-standard
git status --short --untracked-files=all
git diff --cached --stat
```

The ignored-but-tracked command should print nothing. Review every staged file
locally, without pasting possible credentials into public logs or chats. CI scans
history on push/PR, but it runs **after** a push, so it cannot prevent the first
exposure. An example local pre-commit check is:

```bash
gitleaks git . --pre-commit --staged --redact=100 --ignore-gitleaks-allow
```

`.gitleaks.toml` extends the default detector with wallet signing-key, Solana
keypair-array, recovery-phrase, project-token, and token-bearing URL checks.
Detection is defense in depth, not proof that every possible secret format is
covered. Public addresses and transaction hashes do not need blanket removal.

## If a real secret was ever committed

1. Revoke/rotate API and cloud credentials before publication. If a wallet seed
   or signing key was exposed, migrate assets and signing authority to a new
   wallet; deleting the file does not make the old key safe again.
2. Remove the secret from every affected branch and tag using a history-rewrite
   tool such as `git filter-repo`. Coordinate any force-push with collaborators.
3. Rescan the rewritten history and the final staged contents. Existing clones,
   forks, caches, and previously downloaded archives can retain the old data.

Adding a path to `.gitignore`, or running `git rm --cached`, does **not** remove
it from earlier commits. Do not rely on a private-to-public visibility change to
sanitize history.
