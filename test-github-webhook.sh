#!/bin/bash
# Quick test script for GitHub webhook trigger

set -e

echo "🧪 Testing GitHub Actions webhook trigger..."
echo ""

# Check environment variables
if [ -z "$GITHUB_TOKEN" ]; then
    echo "❌ GITHUB_TOKEN not set!"
    echo "Run: export GITHUB_TOKEN='github_pat_YOUR_TOKEN'"
    exit 1
fi

if [ -z "$GITHUB_REPO_OWNER" ]; then
    export GITHUB_REPO_OWNER="kooroshkz"
fi

if [ -z "$GITHUB_REPO_NAME" ]; then
    export GITHUB_REPO_NAME="adaptive-data-profiling-etl"
fi

echo "📝 Configuration:"
echo "  Repository: $GITHUB_REPO_OWNER/$GITHUB_REPO_NAME"
echo "  Token: ${GITHUB_TOKEN:0:20}..."
echo ""

# Make the webhook call
python3 << 'EOF'
import requests
import os
import sys

github_token = os.getenv('GITHUB_TOKEN')
repo_owner = os.getenv('GITHUB_REPO_OWNER')
repo_name = os.getenv('GITHUB_REPO_NAME')

url = f'https://api.github.com/repos/{repo_owner}/{repo_name}/dispatches'
headers = {
    'Authorization': f'token {github_token}',
    'Accept': 'application/vnd.github.v3+json'
}
payload = {
    'event_type': 'trigger-dbt-transform',
    'client_payload': {
        'triggered_by': 'manual_test',
        'workflow': 'test_script',
        'timestamp': 'testing'
    }
}

print("🌐 Calling GitHub API...")
response = requests.post(url, headers=headers, json=payload)

print(f"📊 Response status: {response.status_code}")

if response.status_code == 204:
    print("✅ SUCCESS! Workflow triggered")
    print("")
    print("🔍 Check workflow status at:")
    print(f"   https://github.com/{repo_owner}/{repo_name}/actions")
    sys.exit(0)
elif response.status_code == 401:
    print("❌ FAILED: Unauthorized (401)")
    print("   - Token expired or invalid permissions")
    print("   - Regenerate token with 'Actions: Read and write'")
    sys.exit(1)
elif response.status_code == 404:
    print("❌ FAILED: Not found (404)")
    print(f"   - Check repository: {repo_owner}/{repo_name}")
    print("   - Token might not have access to this repo")
    sys.exit(1)
else:
    print(f"❌ FAILED: {response.status_code}")
    print(f"   Response: {response.text}")
    sys.exit(1)
EOF
