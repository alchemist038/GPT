#!/usr/bin/env python3
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

SCOPES = ["https://www.googleapis.com/auth/youtube"]

CLIENT_SECRET = Path("/media/sf_REC/keys/youtube/client_secret.json")
TOKEN_PATH   = Path("/media/sf_REC/keys/youtube/token.json")

def main():
    if not CLIENT_SECRET.exists():
        raise SystemExit(f"client_secret.json not found: {CLIENT_SECRET}")

    # すでに token.json がある場合は上書きしない（事故防止）
    if TOKEN_PATH.exists():
        print(f"token already exists: {TOKEN_PATH}")
        print("If you want to re-auth, delete token.json and run again.")
        return

    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET), SCOPES)
    creds: Credentials = flow.run_local_server(port=0)

    TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
    print("OK: token saved ->", TOKEN_PATH)

if __name__ == "__main__":
    main()
