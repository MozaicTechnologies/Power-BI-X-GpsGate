
import os

class Config:
    # Get SECRET_KEY from environment, fallback to default
    SECRET_KEY = os.getenv("SECRET_KEY", "jjkhjhkjvkvjhvjfjcjvhkhgcfjgccg")
    
    # Get DATABASE_URL from environment (required)
    raw_url = os.getenv("DATABASE_URL")
    
    if not raw_url:
        print("[ERROR] DATABASE_URL environment variable is not set!")
        print("[ERROR] Please set DATABASE_URL in Render environment variables")
        print("[ERROR] Using fallback SQLite for local development...")
        # Fallback for local development only
        raw_url = "sqlite:////tmp/render.db"
    
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
    
    # GpsGate API settings
    TOKEN = os.getenv("TOKEN")
    BASE_URL = os.getenv("BASE_URL", "https://omantracking2.com")
    
    # Google Drive settings from environment
    GDRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")
    GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    
    # Debug info
    print(f"[CONFIG] GDRIVE_FOLDER_ID: {GDRIVE_FOLDER_ID}")
    print(f"[CONFIG] GOOGLE_APPLICATION_CREDENTIALS: {'Set' if GOOGLE_APPLICATION_CREDENTIALS else 'Not set'}")
    print(f"[CONFIG] SECRET_KEY: {'Set' if SECRET_KEY else 'Using default'}")
    
    # Debug TOKEN (masked for security)
    if TOKEN:
        token_preview = f"{TOKEN[:10]}...{TOKEN[-10:]}" if len(TOKEN) > 20 else "SHORT_TOKEN"
        print(f"[CONFIG] TOKEN: {token_preview} (length: {len(TOKEN)})")
    else:
        print(f"[CONFIG] TOKEN: NOT SET!")