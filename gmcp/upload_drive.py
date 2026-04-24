#!/usr/bin/env python3
"""
Upload file to Google Drive from CLI.

Usage:
    python upload_drive.py --file /path/to/file.pptx
    python upload_drive.py --file /path/to/file.pptx --folder FOLDER_ID
    python upload_drive.py --file /path/to/file.pptx --title "Custom Name.pptx"
"""

import argparse
import os
import sys

from dotenv import load_dotenv
load_dotenv()

from google_tools import drive_upload_file


def main():
    default_folder = os.environ.get("FOLDER_ID", "")
    parser = argparse.ArgumentParser(description="Upload file to Google Drive")
    parser.add_argument("--file", required=True, help="Local file path to upload")
    folder_help = "Google Drive folder ID (default: %s)" % (default_folder or "none")
    parser.add_argument("--folder", default=default_folder, help=folder_help)
    parser.add_argument("--title", default="", help="Override filename (optional)")
    args = parser.parse_args()

    try:
        result = drive_upload_file(
            file_path=args.file,
            folder_id=args.folder,
            title=args.title,
        )
        print(result)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
