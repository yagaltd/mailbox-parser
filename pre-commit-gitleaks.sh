#!/bin/sh
# Gitleaks pre-commit hook
# Scans staged changes for secrets and PII before allowing commit
# Install: place this in .git/hooks/pre-commit and make executable

echo "🔍 Scanning staged changes for secrets and PII..."

docker run --rm -v "$(pwd):/repo" zricethezav/gitleaks:latest \
  git /repo --staged --config /repo/.gitleaks.toml --verbose 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
  echo ""
  echo "❌ Commit BLOCKED: Potential secret or PII detected in staged changes."
  echo "   Review findings above and fix before committing."
  echo "   If this is a false positive, add the fingerprint to .gitleaksignore"
  exit 1
fi

echo "✅ No secrets or PII detected in staged changes."
exit 0
