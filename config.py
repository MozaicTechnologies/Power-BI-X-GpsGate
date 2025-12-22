# import os, os.path

# class Config:
#     SECRET_KEY = "jjkhjhkjvkvjhvjfjcjvhkhgcfjgccg"
#     basedir = os.path.abspath(os.path.dirname(__file__))

#     raw_url = os.getenv("DATABASE_URL", "")

#     # Normalize ANY postgres-y URL to psycopg v3:
#     #  - postgres://...         -> postgresql+psycopg://...
#     #  - postgresql://...       -> postgresql+psycopg://...
#     if raw_url.startswith("postgres://"):
#         raw_url = raw_url.replace("postgres://", "postgresql+psycopg://", 1)
#     elif raw_url.startswith("postgresql://") and not raw_url.startswith("postgresql+psycopg://"):
#         raw_url = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)

#     default_sqlite = os.path.join(basedir, "instance", "render.db")
#     SQLALCHEMY_DATABASE_URI = raw_url or f"sqlite:///{default_sqlite}"
#     SQLALCHEMY_TRACK_MODIFICATIONS = False

#     GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")
#     GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    #------------------------------------------------------------

    ## SQLALCHEMY_DATABASE_URI = f"sqlite:///{db_path}"
    ## SQLALCHEMY_TRACK_MODIFICATIONS = False


    # import os

    # class Config:
    #     SECRET_KEY = "jjkhjhkjvkvjhvjfjcjvhkhgcfjgccg"

    #     # Use writable directory on Render
    #     db_path = '/tmp/render.db'  # Render allows writing only in /tmp

    #     SQLALCHEMY_DATABASE_URI = f"sqlite:///{db_path}"
    #     SQLALCHEMY_TRACK_MODIFICATIONS = False

    #------------------------------------------------------------

import os

class Config:
    # Get SECRET_KEY from environment, fallback to default
    SECRET_KEY = os.getenv("SECRET_KEY", "jjkhjhkjvkvjhvjfjcjvhkhgcfjgccg")
    
    # Get DATABASE_URL from environment (required)
    raw_url = os.getenv("DATABASE_URL")
    
    if not raw_url:
        raise ValueError("DATABASE_URL environment variable is required")
    
    # Normalize ANY postgres-y URL to SQLAlchemy+psycopg format:
    #  - postgres://...         -> postgresql+psycopg://...
    #  - postgresql://...       -> postgresql+psycopg://...
    if raw_url.startswith("postgres://"):
        raw_url = raw_url.replace("postgres://", "postgresql+psycopg://", 1)
    elif raw_url.startswith("postgresql://") and not raw_url.startswith("postgresql+psycopg://"):
        raw_url = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    
    SQLALCHEMY_DATABASE_URI = raw_url
    print(f"[CONFIG] Using PostgreSQL: {raw_url[:50]}...")
    
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Google Drive settings from environment
    GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")
    GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    
    # Debug info
    print(f"[CONFIG] GDRIVE_FOLDER_ID: {GDRIVE_FOLDER_ID}")
    print(f"[CONFIG] GOOGLE_APPLICATION_CREDENTIALS: {'Set' if GOOGLE_APPLICATION_CREDENTIALS else 'Not set'}")
    print(f"[CONFIG] SECRET_KEY: {'Set' if SECRET_KEY else 'Using default'}")