#!/bin/sh
# ────────────────────────────────────────────────
# pre-push.sh — Local safety check before pushing to GitHub
# Runs gitleaks PII/secret scan on the full working tree
#
# Usage: ./pre-push.sh
# ────────────────────────────────────────────────

set -e

echo "=========================================="
echo "🔍 Pre-push safety check"
echo "=========================================="

# Find gitleaks
GITLEAKS=$(command -v gitleaks 2>/dev/null || echo "")
if [ -z "$GITLEAKS" ]; then
  GITLEAKS=$(command -v ~/.local/bin/gitleaks 2>/dev/null || echo "")
fi
if [ -z "$GITLEAKS" ]; then
  echo "❌ gitleaks not found."
  echo "   Install: curl -fsSL https://github.com/gitleaks/gitleaks/releases/download/v8.30.1/gitleaks_8.30.1_linux_x64.tar.gz | tar -xz gitleaks && mv gitleaks ~/.local/bin/"
  exit 1
fi
echo "✅ gitleaks $($GITLEAKS version)"

# Current tree scan
echo ""
echo "--- Scanning working tree ---"
$GITLEAKS detect --no-git --config .gitleaks.toml --verbose -s .
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
  echo ""
  echo "❌ BLOCKED: Secrets or PII detected."
  echo "   Fix findings then re-run: ./pre-push.sh"
  exit 1
fi

# Staged changes scan
echo ""
echo "--- Scanning staged changes ---"
$GITLEAKS git --staged . --config .gitleaks.toml --verbose
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
  echo ""
  echo "❌ BLOCKED: Secrets or PII in staged changes."
  exit 1
fi

echo ""
echo "=========================================="
echo "✅ All checks passed — safe to push!"
echo "=========================================="
exit 0
