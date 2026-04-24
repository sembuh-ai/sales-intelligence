"""
Google API tool implementations for Gmail and Google Drive.
These functions are called by Claude when it selects the appropriate tool.
"""

import base64
import mimetypes
import os
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders

from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SA_KEY_PATH = os.path.join(os.path.dirname(__file__), "service_account", "big-pact-477806-f8-e75a379093f5.json")
TOKEN_PATH = os.path.join(os.path.dirname(__file__), "token.json")

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/drive",
]

# For Gmail with SA, need domain-wide delegation + impersonated user email
IMPERSONATE_EMAIL = os.environ.get("GOOGLE_IMPERSONATE_EMAIL", "")


def _get_creds():
    # Prefer OAuth2 user token if available
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
        if creds and creds.valid:
            return creds
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_PATH, "w") as f:
                f.write(creds.to_json())
            return creds

    # Fallback to service account
    creds = service_account.Credentials.from_service_account_file(SA_KEY_PATH, scopes=SCOPES)
    if IMPERSONATE_EMAIL:
        creds = creds.with_subject(IMPERSONATE_EMAIL)
    return creds


def _gmail():
    return build("gmail", "v1", credentials=_get_creds())


def _drive():
    return build("drive", "v3", credentials=_get_creds())


# ─── Gmail Tools ───────────────────────────────────────────────

def gmail_search_threads(query: str, max_results: int = 10) -> str:
    """Search Gmail threads matching a query."""
    svc = _gmail()
    res = svc.users().threads().list(userId="me", q=query, maxResults=max_results).execute()
    threads = res.get("threads", [])
    if not threads:
        return "No threads found."

    output = []
    for t in threads:
        thread = svc.users().threads().get(userId="me", id=t["id"], format="metadata",
                                           metadataHeaders=["Subject", "From", "Date"]).execute()
        msgs = thread.get("messages", [])
        if msgs:
            headers = {h["name"]: h["value"] for h in msgs[0].get("payload", {}).get("headers", [])}
            output.append(
                f"Thread: {t['id']}\n"
                f"  Subject: {headers.get('Subject', '(no subject)')}\n"
                f"  From: {headers.get('From', 'unknown')}\n"
                f"  Date: {headers.get('Date', 'unknown')}\n"
                f"  Messages: {len(msgs)}"
            )
    return "\n\n".join(output)


def gmail_read_thread(thread_id: str) -> str:
    """Read full content of a Gmail thread."""
    svc = _gmail()
    thread = svc.users().threads().get(userId="me", id=thread_id, format="full").execute()
    msgs = thread.get("messages", [])
    output = []
    for msg in msgs:
        headers = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
        body = _extract_body(msg.get("payload", {}))
        output.append(
            f"From: {headers.get('From', 'unknown')}\n"
            f"Date: {headers.get('Date', 'unknown')}\n"
            f"Subject: {headers.get('Subject', '(no subject)')}\n"
            f"---\n{body}"
        )
    return "\n\n===\n\n".join(output)


def _extract_body(payload: dict) -> str:
    """Extract text body from Gmail message payload."""
    if payload.get("mimeType") == "text/plain" and payload.get("body", {}).get("data"):
        return base64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")

    for part in payload.get("parts", []):
        result = _extract_body(part)
        if result:
            return result
    return "(no text body)"


def gmail_create_draft(to: str, subject: str, body: str, cc: str = "", bcc: str = "", attachments: list[str] | None = None) -> str:
    """Create a Gmail draft, optionally with file attachments."""
    svc = _gmail()

    if attachments:
        msg = MIMEMultipart()
        msg.attach(MIMEText(body))
        for filepath in attachments:
            filepath = os.path.expanduser(filepath)
            if not os.path.isfile(filepath):
                return f"Error: file not found: {filepath}"
            content_type, _ = mimetypes.guess_type(filepath)
            if content_type is None:
                content_type = "application/octet-stream"
            main_type, sub_type = content_type.split("/", 1)
            with open(filepath, "rb") as f:
                part = MIMEBase(main_type, sub_type)
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", "attachment", filename=os.path.basename(filepath))
            msg.attach(part)
    else:
        msg = MIMEText(body)

    msg["to"] = to
    msg["subject"] = subject
    if cc:
        msg["cc"] = cc
    if bcc:
        msg["bcc"] = bcc

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    draft = svc.users().drafts().create(userId="me", body={"message": {"raw": raw}}).execute()
    return f"Draft created. ID: {draft['id']}"


def gmail_send_email(to: str, subject: str, body: str, cc: str = "", bcc: str = "") -> str:
    """Send an email via Gmail."""
    svc = _gmail()
    msg = MIMEText(body)
    msg["to"] = to
    msg["subject"] = subject
    if cc:
        msg["cc"] = cc
    if bcc:
        msg["bcc"] = bcc

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    sent = svc.users().messages().send(userId="me", body={"raw": raw}).execute()
    return f"Email sent. Message ID: {sent['id']}"


def gmail_list_labels() -> str:
    """List all Gmail labels."""
    svc = _gmail()
    results = svc.users().labels().list(userId="me").execute()
    labels = results.get("labels", [])
    return "\n".join(f"- {l['name']} (id: {l['id']})" for l in labels)


# ─── Google Drive Tools ───────────────────────────────────────

def drive_search_files(query: str, max_results: int = 10) -> str:
    """Search Google Drive files. Query uses Drive API query syntax."""
    svc = _drive()
    res = svc.files().list(
        q=query,
        pageSize=max_results,
        fields="files(id, name, mimeType, modifiedTime, owners)",
    ).execute()
    files = res.get("files", [])
    if not files:
        return "No files found."

    output = []
    for f in files:
        owners = ", ".join(o.get("displayName", "?") for o in f.get("owners", []))
        output.append(
            f"- {f['name']}\n"
            f"  ID: {f['id']}\n"
            f"  Type: {f['mimeType']}\n"
            f"  Modified: {f.get('modifiedTime', '?')}\n"
            f"  Owner: {owners}"
        )
    return "\n".join(output)


def drive_list_recent(max_results: int = 10) -> str:
    """List recent Google Drive files."""
    svc = _drive()
    res = svc.files().list(
        pageSize=max_results,
        orderBy="modifiedTime desc",
        fields="files(id, name, mimeType, modifiedTime)",
    ).execute()
    files = res.get("files", [])
    if not files:
        return "No files found."

    return "\n".join(
        f"- {f['name']} ({f['mimeType']}) — modified {f.get('modifiedTime', '?')} — id:{f['id']}"
        for f in files
    )


def drive_read_file(file_id: str) -> str:
    """Read content of a Google Drive file (exports Google Docs as plain text)."""
    svc = _drive()
    meta = svc.files().get(fileId=file_id, fields="mimeType,name").execute()
    mime = meta["mimeType"]

    google_export_map = {
        "application/vnd.google-apps.document": "text/plain",
        "application/vnd.google-apps.spreadsheet": "text/csv",
        "application/vnd.google-apps.presentation": "text/plain",
    }

    if mime in google_export_map:
        content = svc.files().export(fileId=file_id, mimeType=google_export_map[mime]).execute()
        if isinstance(content, bytes):
            content = content.decode("utf-8", errors="replace")
        return f"File: {meta['name']}\nType: {mime}\n---\n{content}"
    else:
        content = svc.files().get_media(fileId=file_id).execute()
        if isinstance(content, bytes):
            try:
                content = content.decode("utf-8", errors="replace")
            except Exception:
                return f"File: {meta['name']} — binary file, {len(content)} bytes"
        return f"File: {meta['name']}\nType: {mime}\n---\n{content}"


def drive_create_file(title: str, content: str = "", mime_type: str = "text/plain", folder_id: str = "") -> str:
    """Create a file in Google Drive."""
    from googleapiclient.http import MediaInMemoryUpload

    svc = _drive()
    metadata = {"name": title}
    if folder_id:
        metadata["parents"] = [folder_id]

    if content:
        media = MediaInMemoryUpload(content.encode("utf-8"), mimetype=mime_type, resumable=False)
        f = svc.files().create(body=metadata, media_body=media, fields="id,name,webViewLink").execute()
    else:
        google_types = {
            "document": "application/vnd.google-apps.document",
            "spreadsheet": "application/vnd.google-apps.spreadsheet",
            "presentation": "application/vnd.google-apps.presentation",
            "folder": "application/vnd.google-apps.folder",
        }
        metadata["mimeType"] = google_types.get(mime_type, mime_type)
        f = svc.files().create(body=metadata, fields="id,name,webViewLink").execute()

    link = f.get("webViewLink", "no link")
    return f"Created: {f['name']} (id: {f['id']})\nLink: {link}"


def drive_upload_file(file_path: str, folder_id: str = "", title: str = "") -> str:
    """Upload a local file to Google Drive."""
    from googleapiclient.http import MediaFileUpload

    file_path = os.path.abspath(os.path.expanduser(file_path))
    if not os.path.isfile(file_path):
        return f"Error: file not found: {file_path}"

    svc = _drive()
    filename = title or os.path.basename(file_path)
    content_type, _ = mimetypes.guess_type(file_path)
    if content_type is None:
        content_type = "application/octet-stream"

    metadata = {"name": filename}
    if folder_id:
        metadata["parents"] = [folder_id]

    media = MediaFileUpload(file_path, mimetype=content_type, resumable=True)
    f = svc.files().create(body=metadata, media_body=media, fields="id,name,webViewLink").execute()
    link = f.get("webViewLink", "no link")
    return f"Uploaded: {f['name']} (id: {f['id']})\nLink: {link}"


# ─── Tool Registry (for Claude API) ───────────────────────────

TOOL_DEFINITIONS = [
    {
        "name": "gmail_search_threads",
        "description": "Search Gmail threads. Use Gmail search syntax (e.g. 'from:user@example.com', 'is:unread', 'subject:hello', 'newer_than:7d').",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Gmail search query"},
                "max_results": {"type": "integer", "description": "Max threads to return (default 10)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "gmail_read_thread",
        "description": "Read full content of a specific Gmail thread by thread ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "thread_id": {"type": "string", "description": "Gmail thread ID"},
            },
            "required": ["thread_id"],
        },
    },
    {
        "name": "gmail_create_draft",
        "description": "Create a Gmail draft email.",
        "input_schema": {
            "type": "object",
            "properties": {
                "to": {"type": "string", "description": "Recipient email"},
                "subject": {"type": "string", "description": "Email subject"},
                "body": {"type": "string", "description": "Email body text"},
                "cc": {"type": "string", "description": "CC recipients (comma-separated)"},
                "bcc": {"type": "string", "description": "BCC recipients (comma-separated)"},
            },
            "required": ["to", "subject", "body"],
        },
    },
    {
        "name": "gmail_send_email",
        "description": "Send an email via Gmail.",
        "input_schema": {
            "type": "object",
            "properties": {
                "to": {"type": "string", "description": "Recipient email"},
                "subject": {"type": "string", "description": "Email subject"},
                "body": {"type": "string", "description": "Email body text"},
                "cc": {"type": "string", "description": "CC recipients (comma-separated)"},
                "bcc": {"type": "string", "description": "BCC recipients (comma-separated)"},
            },
            "required": ["to", "subject", "body"],
        },
    },
    {
        "name": "gmail_list_labels",
        "description": "List all Gmail labels.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "drive_search_files",
        "description": "Search Google Drive files. Use Drive query syntax (e.g. \"name contains 'report'\", \"mimeType='application/pdf'\").",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Drive search query"},
                "max_results": {"type": "integer", "description": "Max files to return (default 10)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "drive_list_recent",
        "description": "List recent Google Drive files, sorted by last modified.",
        "input_schema": {
            "type": "object",
            "properties": {
                "max_results": {"type": "integer", "description": "Max files to return (default 10)"},
            },
        },
    },
    {
        "name": "drive_read_file",
        "description": "Read content of a Google Drive file by file ID. Google Docs are exported as plain text, Sheets as CSV.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_id": {"type": "string", "description": "Google Drive file ID"},
            },
            "required": ["file_id"],
        },
    },
    {
        "name": "drive_create_file",
        "description": "Create a file in Google Drive. For empty Google Docs/Sheets/Slides, set mime_type to 'document'/'spreadsheet'/'presentation'. For folders, use 'folder'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "File name"},
                "content": {"type": "string", "description": "File content (text). Leave empty for Google native types."},
                "mime_type": {"type": "string", "description": "MIME type or shorthand (document/spreadsheet/presentation/folder). Default: text/plain"},
                "folder_id": {"type": "string", "description": "Parent folder ID (optional)"},
            },
            "required": ["title"],
        },
    },
    {
        "name": "drive_upload_file",
        "description": "Upload a local file to Google Drive. Supports any file type.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string", "description": "Local file path to upload"},
                "folder_id": {"type": "string", "description": "Parent folder ID (optional)"},
                "title": {"type": "string", "description": "Override filename (optional)"},
            },
            "required": ["file_path"],
        },
    },
]

# Map tool names to functions
TOOL_FUNCTIONS = {
    "gmail_search_threads": gmail_search_threads,
    "gmail_read_thread": gmail_read_thread,
    "gmail_create_draft": gmail_create_draft,
    "gmail_send_email": gmail_send_email,
    "gmail_list_labels": gmail_list_labels,
    "drive_search_files": drive_search_files,
    "drive_list_recent": drive_list_recent,
    "drive_read_file": drive_read_file,
    "drive_create_file": drive_create_file,
    "drive_upload_file": drive_upload_file,
}
