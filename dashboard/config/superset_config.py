import os

# Superset secret key – must be set via environment variable SUPERSET_SECRET_KEY
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")

# SQLAlchemy DB URI – supplied via SQLALCHEMY_DATABASE_URI environment variable
SQLALCHEMY_DATABASE_URI = os.environ.get("SQLALCHEMY_DATABASE_URI")

