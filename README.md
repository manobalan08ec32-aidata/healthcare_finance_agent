# requirements.txt additions
# Add these to ensure proper session handling
gunicorn==20.1.0
streamlit>=1.28.0

# Azure App Service Settings (Add these in Azure Portal > Configuration > Application Settings)

# 1. DISABLE OVERLAPPED RECYCLING (Critical for session isolation)
WEBSITES_DISABLE_OVERLAPPED_RECYCLING=1

# 2. ENSURE SINGLE INSTANCE (for testing)
WEBSITES_INSTANCE_COUNT=1

# 3. SET SESSION AFFINITY (if using multiple instances)
ARR_DISABLE_SESSION_AFFINITY=false

# 4. MEMORY SETTINGS
WEBSITES_MEMORY_USAGE_THRESHOLD=90

# 5. STREAMLIT SPECIFIC
STREAMLIT_SERVER_HEADLESS=true
STREAMLIT_SERVER_PORT=8000
STREAMLIT_SERVER_ADDRESS=0.0.0.0

# 6. DISABLE CACHING IN PRODUCTION (for debugging)
STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# Startup Command (in Azure Portal > Configuration > General Settings)
# python -m streamlit run main.py --server.port=8000 --server.address=0.0.0.0 --server.headless=true --server.enableCORS=false --server.enableXsrfProtection=false

# Additional configuration file: .streamlit/config.toml
[server]
headless = true
enableCORS = false
enableXsrfProtection = false
port = 8000
address = "0.0.0.0"

[browser]
gatherUsageStats = false
serverAddress = "0.0.0.0"
serverPort = 8000

[global]
developmentMode = false
