# Setup pyenv for Python version management
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init - bash)"

# Activate virtual environment
cd backend
source .venv/bin/activate

# Run backend server
python main.py
