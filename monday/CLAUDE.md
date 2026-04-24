# PushToProd Backend

Python FastAPI service that extracts PDF data via Claude and integrates with Monday.com.

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env  # fill in your keys
python app.py
```

Server runs on http://localhost:8000. Docs at /docs.

## Monday.com MCP

MCP server configured in `.mcp.json`. Needs `MONDAY_API_KEY` and `MONDAY_WORKSPACE_NAME` env vars.

Available Monday tools: `monday_list_workspaces`, `monday_list_boards`, `monday_get_board_groups`, `monday_get_board_columns`, `monday_create_item`, `monday_update_item`, `monday_create_update`, `monday_delete_item`, etc.

## Endpoints

- `GET /health` — health check
- `POST /extract` — upload PDF, get extracted data
- `POST /extract-and-push` — extract PDF + prepare Monday.com item
