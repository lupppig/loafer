#!/usr/bin/env bash

set -e

echo "🔍 Checking virtual environment..."

# 1. Check if .venv exists
if [ -d "../.venv" ]; then
    echo "⚠️ Existing .venv found"

    # Check if python inside it exists
    if [ ! -f ".venv/bin/python" ]; then
        echo "❌ Broken .venv detected (missing python)"
        echo "🧹 Removing broken environment..."
        rm -rf .venv
    else
        echo "✅ .venv looks valid"
    fi
fi

# 2. Recreate if missing
if [ ! -d ".venv" ]; then
    echo "🚀 Creating new virtual environment..."
    python3 -m venv .venv
fi

# 3. Activate
echo "⚙️ Activating environment..."
source .venv/bin/activate

# 4. Upgrade pip
echo "⬆️ Upgrading pip..."
pip install --upgrade pip

# 5. Install dependencies
if [ -f "pyproject.toml" ]; then
    echo "📦 Installing via pyproject (uv/pip)..."
    pip install .
elif [ -f "requirements.txt" ]; then
    echo "📦 Installing requirements.txt..."
    pip install -r requirements.txt
else
    echo "⚠️ No dependency file found"
fi

# 6. Fix VSCode interpreter
echo "🛠 Fixing VSCode settings..."
mkdir -p .vscode

cat > .vscode/settings.json <<EOF
{
    "python.defaultInterpreterPath": "\${workspaceFolder}/.venv/bin/python"
}
EOF

echo "✅ Done. Use: source .venv/bin/activate"