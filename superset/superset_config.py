import os

# Superset specific config
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# Flask App Builder configuration
SECRET_KEY = 'lakehouse_superset_secret_key_change_in_production'

# Database Configuration
SQLALCHEMY_DATABASE_URI = 'postgresql://airflow:airflow@postgres:5432/superset'

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = ''

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}