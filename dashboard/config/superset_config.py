import os

# Superset secret key - must be set via environment variable SUPERSET_SECRET_KEY
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "atmosync_dev_secret_key_change_in_production")

# SQLAlchemy DB URI for Superset metadata database (PostgreSQL)
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SQLALCHEMY_DATABASE_URI",
    "postgresql+psycopg2://superset:superset@postgres:5432/superset",
)

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
}

# Webserver address and port
SUPERSET_WEBSERVER_ADDRESS = "0.0.0.0"
SUPERSET_WEBSERVER_PORT = 8088

# Snowflake SQLAlchemy Connection URI format for reference:
# snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}
