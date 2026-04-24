#!/usr/bin/env python3
"""
Create Gmail draft from CLI.

Usage:
    python create_draft.py --to user@example.com --subject "Hello" --body "Message here"
    python create_draft.py --to user@example.com --subject "Hello" --body "Message" --cc a@b.com --bcc c@d.com
"""

import argparse
import sys

from dotenv import load_dotenv
load_dotenv()

from google_tools import gmail_create_draft


def main():
    parser = argparse.ArgumentParser(description="Create Gmail draft")
    parser.add_argument("--to", required=True, help="Recipient email")
    parser.add_argument("--subject", required=True, help="Email subject")
    parser.add_argument("--body", required=True, help="Email body")
    parser.add_argument("--cc", default="", help="CC recipients (comma-separated)")
    parser.add_argument("--bcc", default="", help="BCC recipients (comma-separated)")
    parser.add_argument("--attach", nargs="+", default=None, help="File paths to attach")
    args = parser.parse_args()

    try:
        result = gmail_create_draft(
            to=args.to,
            subject=args.subject,
            body=args.body,
            cc=args.cc,
            bcc=args.bcc,
            attachments=args.attach,
        )
        print(result)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
