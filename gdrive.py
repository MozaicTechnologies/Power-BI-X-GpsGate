# gdrive.py
import io, os
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseUpload

SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
DEFAULT_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID")

def _service():
    if not SERVICE_ACCOUNT_FILE:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS env var not set")
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)

def upload_bytes_as_csv(file_bytes: bytes, filename: str, folder_id: str | None = None):
    srv = _service()
    metadata = {"name": filename}
    if folder_id or DEFAULT_FOLDER_ID:
        metadata["parents"] = [folder_id or DEFAULT_FOLDER_ID]

    media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype="text/csv", resumable=False)
    created = srv.files().create(
        body=metadata,
        media_body=media,
        fields="id, parents",
        supportsAllDrives=True,   # important for Shared Drives
    ).execute()
    file_id = created["id"]

    # Make public (optional)
    srv.permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": "reader"},
        supportsAllDrives=True,   # also fine for Shared Drives
    ).execute()

    direct_link = f"https://drive.google.com/uc?export=download&id={file_id}"
    return file_id, direct_link
