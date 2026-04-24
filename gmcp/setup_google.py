"""
One-time setup: generates Google OAuth credentials.
Run this first: python setup_google.py

Requires a credentials.json from Google Cloud Console with Gmail + Drive APIs enabled.
Scopes: Gmail (modify) + Drive (full).
"""

import os
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/drive",
]

TOKEN_PATH = os.path.join(os.path.dirname(__file__), "token.json")
CREDS_PATH = os.path.join(os.path.dirname(__file__), "credentials.json")


def authenticate():
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDS_PATH):
                print(f"ERROR: {CREDS_PATH} not found.")
                print("Download it from Google Cloud Console → APIs & Services → Credentials → OAuth 2.0 Client ID")
                return None
            flow = InstalledAppFlow.from_client_secrets_file(CREDS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_PATH, "w") as f:
            f.write(creds.to_json())
        print(f"Token saved to {TOKEN_PATH}")

    return creds


if __name__ == "__main__":
    creds = authenticate()
    if creds:
        print("Authentication successful!")
