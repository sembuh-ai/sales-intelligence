#!/usr/bin/env python3
"""
Sales Intelligence — Unified Automation
Combines Monday.com CRM operations with document generation pipeline.

Commands:
    python app.py generate              — Fetch deals → generate docs → upload → email → Slack
    python app.py generate --deal "X"   — Generate docs for one deal only
    python app.py mom <file.docx>       — Extract MoM and update Monday.com CRM
    python app.py mom <file.docx> --dry-run
    python app.py health                — Run deal health scoring
    python app.py interactive           — Interactive Monday.com assistant
"""

import asyncio
import argparse
import base64
import copy
import datetime
import json
import os
import re
import sys
from contextlib import AsyncExitStack

import requests
from anthropic import Anthropic
from dotenv import load_dotenv

# ── Load env from all sources ─────────────────────────────────
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_BASE_DIR, ".env"))
load_dotenv(os.path.join(_BASE_DIR, "monday", ".env"), override=False)
load_dotenv(os.path.join(_BASE_DIR, "gmcp", ".env"), override=False)

# ── Config ────────────────────────────────────────────────────
MONDAY_API_KEY = os.getenv("MONDAY_API_KEY", "")
MONDAY_WORKSPACE_ID = os.getenv("MONDAY_WORKSPACE_ID", "")
MONDAY_WORKSPACE_NAME = os.getenv("MONDAY_WORKSPACE_NAME", "")
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-opus-4-6")
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID", "C0AU9FA76EB")
SLACK_NOTIFY_USER = "U08UV06KD45"
GOOGLE_FOLDER_ID = os.getenv("FOLDER_ID", "")

MONDAY_API_URL = "https://api.monday.com/v2"
SLACK_API_URL = "https://slack.com/api"

TEMPLATE_DIR = os.path.join(_BASE_DIR, "PTP Hackathon - Brief")
OUTPUT_DIR = os.path.join(_BASE_DIR, "output")

TODAY = datetime.date.today()
TODAY_STR = TODAY.strftime("%d %B %Y")

# ── Pricing / Product Config ─────────────────────────────────
MARGIN_ASSUMPTIONS = {
    "Claim Workflow": {"unit_price": 1.0, "margin": 0.60, "type": "Recurring", "uom": "Per Claim"},
    "Fraud Detection": {"unit_price": 2.0, "margin": 0.65, "type": "Recurring", "uom": "Per Claim"},
    "Implementation": {"unit_price": 50000, "margin": 0.50, "type": "One Time Fee", "uom": "Per Implementation"},
}

STAGE_WEIGHTS = {
    "Open": 0.10, "First Meeting Done": 0.20, "Piloting": 0.30,
    "Solutioning": 0.50, "Waiting Confirmation": 0.70,
    "[Won] Waiting Signature": 0.90, "Won": 1.00,
    "Implementation Done": 1.00, "Lost": 0.00,
}

HEALTH_RULES = [
    # (rule_name, deduction, check_fn)
    # check_fn(deal, today) -> bool (True = deduction applies)
]


def detect_products(deal_name):
    name_lower = deal_name.lower()
    if "full suite" in name_lower:
        return ["Claim Workflow", "Fraud Detection", "Implementation"]
    if "fwa" in name_lower or "fraud" in name_lower:
        return ["Fraud Detection", "Implementation"]
    if "ocr" in name_lower or "stp" in name_lower:
        return ["Claim Workflow", "Implementation"]
    return ["Claim Workflow", "Fraud Detection", "Implementation"]


def _parse_num(val):
    if not val:
        return 0
    val = str(val).replace(",", "").replace("$", "").strip()
    try:
        return float(val)
    except ValueError:
        return 0


# ══════════════════════════════════════════════════════════════
# MONDAY.COM API (Direct GraphQL)
# ══════════════════════════════════════════════════════════════

def monday_query(query, variables=None):
    headers = {
        "Authorization": MONDAY_API_KEY,
        "Content-Type": "application/json",
        "API-Version": "2024-10",
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    resp = requests.post(MONDAY_API_URL, json=payload, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"Monday API error: {data['errors']}")
    return data["data"]


def fetch_boards():
    query = """
    query ($wsIds: [ID!]) {
        boards(workspace_ids: $wsIds, limit: 50) { id name }
    }
    """
    return monday_query(query, {"wsIds": [int(MONDAY_WORKSPACE_ID)]})["boards"]


def find_deals_board(boards):
    for b in boards:
        if b["name"].strip().lower() == "deals":
            return b
    for b in boards:
        if "deal" in b["name"].lower() and "subitem" not in b["name"].lower():
            return b
    return boards[0] if boards else None


def fetch_deals(board_id):
    query = """
    query ($boardId: [ID!]!) {
        boards(ids: $boardId) {
            items_page(limit: 100) {
                items {
                    id name
                    column_values { id type text value }
                }
            }
        }
    }
    """
    data = monday_query(query, {"boardId": [int(board_id)]})
    items = data["boards"][0]["items_page"]["items"]
    deals = []
    for item in items:
        deal = {"id": item["id"], "name": item["name"]}
        for col in item["column_values"]:
            deal[col["id"]] = col.get("text", "") or ""
            deal[f"{col['id']}_raw"] = col.get("value", "")
        deals.append(deal)
    return deals


def parse_deal(deal):
    name = deal.get("name", "Unknown")
    stage = deal.get("deal_stage", "Open")
    deal_value = _parse_num(deal.get("deal_value", "0"))
    close_date = deal.get("deal_expected_close_date", "")
    monthly_claim_vol = _parse_num(deal.get("numeric_mm1bmx9t", "0"))
    members_covered = _parse_num(deal.get("numeric_mm1bx91m", "0"))
    pricing_model = deal.get("dropdown_mm1b79r5", "Per Claim")
    owner = deal.get("deal_owner", "")
    proposal_date = deal.get("date_mm1bpvvx", "")
    incurred = _parse_num(deal.get("numeric_mm1bdpzy", "0"))
    excess = _parse_num(deal.get("numeric_mm1bkxy8", "0"))
    approved = _parse_num(deal.get("numeric_mm1b64b7", "0"))

    products = detect_products(name)

    # Estimate volume from deal_value when not set
    if monthly_claim_vol == 0 and deal_value > 0:
        impl_fee = 50000 if "Implementation" in products else 0
        recurring_rate = sum(
            MARGIN_ASSUMPTIONS[p]["unit_price"] for p in products if p != "Implementation"
        ) or 1
        annual_claim_vol = max(int((deal_value - impl_fee) / recurring_rate), 1200) if deal_value > impl_fee else 12000
        monthly_claim_vol = annual_claim_vol / 12
    else:
        annual_claim_vol = monthly_claim_vol * 12

    if members_covered == 0 and monthly_claim_vol > 0:
        members_covered = monthly_claim_vol * 10

    return {
        "id": deal.get("id"), "name": name, "stage": stage,
        "deal_value": deal_value, "close_date": close_date,
        "monthly_claim_vol": monthly_claim_vol, "annual_claim_vol": annual_claim_vol,
        "members_covered": members_covered, "pricing_model": pricing_model or "Per Claim",
        "incurred": incurred, "excess": excess, "approved": approved,
        "owner": owner, "proposal_date": proposal_date, "products": products,
    }


# ══════════════════════════════════════════════════════════════
# MONDAY.COM MCP CLIENT (for MoM + Interactive)
# ══════════════════════════════════════════════════════════════

class MondayMCPClient:
    """Claude-powered Monday.com MCP client for complex operations."""

    def __init__(self):
        self.exit_stack = AsyncExitStack()
        self.session = None
        self.anthropic = Anthropic()
        self.tools = []

    async def connect(self):
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        server_params = StdioServerParameters(
            command="npx",
            args=["-y", "verdant-monday-mcp"],
            env={
                **os.environ,
                "MONDAY_API_KEY": MONDAY_API_KEY,
                "MONDAY_WORKSPACE_NAME": MONDAY_WORKSPACE_NAME,
            },
        )
        transport = await self.exit_stack.enter_async_context(stdio_client(server_params))
        read, write = transport
        self.session = await self.exit_stack.enter_async_context(
            __import__("mcp").ClientSession(read, write)
        )
        await self.session.initialize()

        response = await self.session.list_tools()
        self.tools = [
            {"name": t.name, "description": t.description or "", "input_schema": t.inputSchema}
            for t in response.tools
        ]
        print(f"MCP connected. {len(self.tools)} Monday tools.", file=sys.stderr)

    async def chat(self, prompt: str, history: list, system: str = "") -> tuple[str, list]:
        history.append({"role": "user", "content": prompt})

        default_system = (
            f"You are a Monday.com CRM assistant for workspace {MONDAY_WORKSPACE_ID}. "
            f"Always pass workspace_ids=[{MONDAY_WORKSPACE_ID}] when listing boards. "
            "Be concise. Use tables/lists where helpful."
        )

        while True:
            response = self.anthropic.messages.create(
                model=CLAUDE_MODEL, max_tokens=4096,
                system=system or default_system,
                messages=history, tools=self.tools,
            )
            assistant_content = list(response.content)
            history.append({"role": "assistant", "content": assistant_content})

            if response.stop_reason == "end_turn":
                return "".join(b.text for b in response.content if b.type == "text"), history

            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                print(f"  calling {block.name}...", file=sys.stderr)

                tool_input = block.input
                if block.name == "monday_create_update" and "updateText" in tool_input:
                    tool_input = {**tool_input, "updateText": tool_input["updateText"].replace("\n", "<br>")}

                try:
                    result = await self.session.call_tool(block.name, tool_input)
                    result_text = " ".join(c.text for c in (result.content or []) if hasattr(c, "text"))
                    tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_text})
                except Exception as e:
                    print(f"  TOOL ERROR: {e}", file=sys.stderr)
                    tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": f"Error: {e}", "is_error": True})

            history.append({"role": "user", "content": tool_results})

    async def cleanup(self):
        await self.exit_stack.aclose()


# ══════════════════════════════════════════════════════════════
# DOCUMENT GENERATORS
# ══════════════════════════════════════════════════════════════

def _unmerge_and_clear(ws):
    for merge in list(ws.merged_cells.ranges):
        ws.unmerge_cells(str(merge))
    for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
        for cell in row:
            cell.value = None


def generate_quotation(deal, seq_num=1):
    import openpyxl

    template_path = os.path.join(TEMPLATE_DIR, "Sembuh AI_Quotation Template.xlsx")
    wb = openpyxl.load_workbook(template_path)
    ws = wb.active

    client_name = deal["name"]
    q_number = f"SBH-Q-{TODAY.year}-{seq_num:03d}"
    validity_date = (TODAY + datetime.timedelta(days=30)).strftime("%d %B %Y")

    products = deal["products"]
    line_items = []
    total_revenue = 0
    for prod_name in products:
        info = MARGIN_ASSUMPTIONS[prod_name]
        vol = 1 if prod_name == "Implementation" else (deal["annual_claim_vol"] or deal["monthly_claim_vol"] * 12)
        revenue = info["unit_price"] * vol
        total_revenue += revenue
        line_items.append({
            "product": prod_name, "type": info["type"],
            "unit_price": info["unit_price"], "volume": vol,
            "uom": info["uom"], "revenue": revenue,
        })

    _unmerge_and_clear(ws)

    ws["A1"] = "SEMBUH AI — QUOTATION"
    ws["A3"], ws["B3"] = "Client:", client_name
    ws["A4"], ws["B4"] = "Date:", TODAY_STR
    ws["A5"], ws["B5"] = "Quotation No:", q_number
    ws["A6"], ws["B6"] = "Validity:", f"30 days (until {validity_date})"
    ws["A7"], ws["B7"] = "Pricing Model:", deal["pricing_model"]

    for col, hdr in enumerate(["No", "Product / Service", "Type", "Unit Price ($)", "Volume", "UoM", "Total ($)"]):
        ws.cell(row=9, column=col + 1, value=hdr)

    for idx, item in enumerate(line_items):
        r = 10 + idx
        ws.cell(row=r, column=1, value=idx + 1)
        ws.cell(row=r, column=2, value=item["product"])
        ws.cell(row=r, column=3, value=item["type"])
        ws.cell(row=r, column=4, value=item["unit_price"])
        ws.cell(row=r, column=5, value=item["volume"])
        ws.cell(row=r, column=6, value=item["uom"])
        ws.cell(row=r, column=7, value=item["revenue"])

    tr = 10 + len(line_items)
    ws.cell(row=tr, column=2, value="TOTAL")
    ws.cell(row=tr, column=7, value=total_revenue)

    tc = tr + 2
    ws[f"A{tc}"] = "Terms & Conditions"
    for i, term in enumerate([
        "1. Payment terms: Net 30 days from invoice date.",
        "2. Prices are in USD and exclude applicable taxes.",
        "3. Recurring fees are billed monthly based on actual volume.",
        "4. Implementation fee is billed upon project kickoff.",
        "5. This quotation is valid for 30 days from the date of issue.",
    ]):
        ws[f"A{tc + 1 + i}"] = term
    ws[f"A{tc + 7}"] = "Prepared by: Sembuh AI Sales Team"
    ws[f"A{tc + 8}"] = f"Date: {TODAY_STR}"

    safe_name = re.sub(r'[^\w\s-]', '', client_name).strip().replace(' ', '_')
    out_path = os.path.join(OUTPUT_DIR, safe_name, f"Sembuh AI - {client_name} - Quotation v1.xlsx")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    wb.save(out_path)
    print(f"    Quotation: {out_path}")
    return out_path


def generate_pricing_internal(deal, all_deals):
    import openpyxl

    template_path = os.path.join(TEMPLATE_DIR, "Hackathon_Pricing Internal.xlsx")
    wb = openpyxl.load_workbook(template_path)
    client_name = deal["name"]
    products = deal["products"]

    # --- Sheet 1: Per-Client ---
    ws1 = wb.worksheets[0]
    ws1.title = client_name[:31]
    _unmerge_and_clear(ws1)

    ws1["A1"], ws1["H1"] = "Per Client Gross Profit", f"Date {TODAY_STR}"
    for col, hdr in enumerate(["Client", "Status", "Total Revenue ($)", "Blended Margin %", "Cost of Revenue ($)", "Gross Profit ($)"]):
        ws1.cell(row=2, column=col + 1, value=hdr)

    total_rev = sum(
        MARGIN_ASSUMPTIONS[p]["unit_price"] * (1 if p == "Implementation" else deal["annual_claim_vol"])
        for p in products
    )
    blended_margin = round(sum(MARGIN_ASSUMPTIONS[p]["margin"] for p in products) / len(products), 2) if products else 0.60

    ws1["A3"], ws1["B3"], ws1["C3"], ws1["D3"] = client_name, deal["stage"], round(total_rev, 2), blended_margin
    ws1["E3"], ws1["F3"] = "=C3*(1-D3)", "=C3*D3"

    # --- Sheet 2: Margin Summary ---
    ws2 = wb.worksheets[1] if len(wb.worksheets) > 1 else wb.create_sheet("Margin Summary")
    _unmerge_and_clear(ws2)

    ws2["A1"] = "Sembuh AI — Gross Profit & Margin Summary"
    ws2["A2"] = "Margin assumptions: Claim Workflow 60% | Fraud Detection 65% | Implementation 50%"
    for col, hdr in enumerate(["Product / Service", "Type", "Unit Price ($)", "Margin %", "Cost per Unit ($)", "Gross Profit per Unit ($)", "Notes"]):
        ws2.cell(row=4, column=col + 1, value=hdr)

    for i, (name, typ, price, margin, notes) in enumerate([
        ("Claim Workflow", "Recurring", 1.0, 0.60, "AI-enabled claim automation"),
        ("Fraud Detection", "Recurring", 2.0, 0.65, "ML inference cost is low"),
        ("Implementation", "One Time Fee", 50000, 0.50, "Engineering + integration"),
    ]):
        r = 5 + i
        ws2[f"A{r}"], ws2[f"B{r}"], ws2[f"C{r}"], ws2[f"D{r}"] = name, typ, price, margin
        ws2[f"E{r}"], ws2[f"F{r}"], ws2[f"G{r}"] = f"=C{r}*(1-D{r})", f"=C{r}*D{r}", notes

    ws2["A10"] = "Per Client Gross Profit"
    for col, hdr in enumerate(["Client", "Status", "Total Revenue ($)", "Blended Margin %", "Cost of Revenue ($)", "Gross Profit ($)", "GP as % of Pipeline"]):
        ws2.cell(row=11, column=col + 1, value=hdr)

    for i, d in enumerate(all_deals):
        r = 12 + i
        rev = sum(MARGIN_ASSUMPTIONS[p]["unit_price"] * (1 if p == "Implementation" else d["annual_claim_vol"]) for p in d["products"])
        bm = round(sum(MARGIN_ASSUMPTIONS[p]["margin"] for p in d["products"]) / len(d["products"]), 2) if d["products"] else 0.6
        ws2[f"A{r}"], ws2[f"B{r}"], ws2[f"C{r}"], ws2[f"D{r}"] = d["name"], d["stage"], round(rev, 2), bm
        ws2[f"E{r}"], ws2[f"F{r}"] = f"=C{r}*(1-D{r})", f"=C{r}*D{r}"

    tr = 12 + len(all_deals)
    ws2[f"A{tr}"] = "TOTAL"
    ws2[f"C{tr}"], ws2[f"D{tr}"] = f"=SUM(C12:C{tr-1})", f"=F{tr}/C{tr}"
    ws2[f"E{tr}"], ws2[f"F{tr}"] = f"=SUM(E12:E{tr-1})", f"=SUM(F12:F{tr-1})"

    # --- Sheet 3: Deal Summary ---
    ws3 = wb.worksheets[2] if len(wb.worksheets) > 2 else wb.create_sheet("Deal Summary")
    _unmerge_and_clear(ws3)
    ws3["A1"] = "Sembuh AI — Potential Client Pipeline"
    ws3["A2"] = "Products & Services Proposal Summary  |  Annual Basis"

    for col, hdr in enumerate(["Client", "Status", "Products", "Monthly Vol", "Annual Vol",
                                "CW $/claim", "FD $/claim", "CW Revenue", "FD Revenue",
                                "Impl Fee", "Total Revenue", "Notes", "Margin %", "Cost", "GP"]):
        ws3.cell(row=4, column=col + 1, value=hdr)

    for i, d in enumerate(all_deals):
        r = 5 + i
        prods = d["products"]
        has_cw, has_fd = "Claim Workflow" in prods, "Fraud Detection" in prods
        bm = round(sum(MARGIN_ASSUMPTIONS[p]["margin"] for p in prods) / len(prods), 2) if prods else 0.6
        ws3[f"A{r}"], ws3[f"B{r}"] = d["name"], d["stage"]
        ws3[f"C{r}"], ws3[f"D{r}"], ws3[f"E{r}"] = " + ".join(prods), d["monthly_claim_vol"], f"=D{r}*12"
        ws3[f"F{r}"] = 1.0 if has_cw else "-"
        ws3[f"G{r}"] = 2.0 if has_fd else "-"
        ws3[f"H{r}"] = f"=E{r}*F{r}" if has_cw else ""
        ws3[f"I{r}"] = f"=E{r}*G{r}" if has_fd else ""
        ws3[f"J{r}"] = 50000 if "Implementation" in prods else ""
        ws3[f"K{r}"], ws3[f"M{r}"] = f"=H{r}+I{r}+J{r}", bm
        ws3[f"N{r}"], ws3[f"O{r}"] = f"=K{r}*(1-M{r})", f"=K{r}*M{r}"

    # --- Sheet 4: Client Detail ---
    ws4 = wb.worksheets[3] if len(wb.worksheets) > 3 else wb.create_sheet("Client Detail")
    _unmerge_and_clear(ws4)
    ws4["A1"] = "Sembuh AI — Per Client Product & Pricing Detail"
    ws4["A2"] = "Annual revenue breakdown per client"

    for col, hdr in enumerate(["Client", "Product", "Type", "Unit Price ($)", "Annual Vol", "UoM", "Revenue ($)", "Notes", "Margin %", "Cost ($)", "GP ($)"]):
        ws4.cell(row=4, column=col + 1, value=hdr)

    detail_notes = {"Claim Workflow": "AI claim workflow", "Fraud Detection": "FWA detection", "Implementation": "Core system integration"}
    row_num = 5
    for d in all_deals:
        for p in d["products"]:
            info = MARGIN_ASSUMPTIONS[p]
            vol = 1 if p == "Implementation" else d["annual_claim_vol"]
            ws4[f"A{row_num}"], ws4[f"B{row_num}"], ws4[f"C{row_num}"] = d["name"], p, info["type"]
            ws4[f"D{row_num}"], ws4[f"E{row_num}"], ws4[f"F{row_num}"] = info["unit_price"], vol, info["uom"]
            ws4[f"G{row_num}"], ws4[f"H{row_num}"], ws4[f"I{row_num}"] = f"=D{row_num}*E{row_num}", detail_notes.get(p, ""), info["margin"]
            ws4[f"J{row_num}"], ws4[f"K{row_num}"] = f"=G{row_num}*(1-I{row_num})", f"=G{row_num}*I{row_num}"
            row_num += 1

    safe_name = re.sub(r'[^\w\s-]', '', client_name).strip().replace(' ', '_')
    out_path = os.path.join(OUTPUT_DIR, safe_name, f"Sembuh AI - {client_name} - Pricing Internal.xlsx")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    wb.save(out_path)
    print(f"    Pricing:   {out_path}")
    return out_path


def generate_proposal(deal):
    from pptx import Presentation

    template_path = os.path.join(TEMPLATE_DIR, "Sembuh AI_Proposal.pptx")
    prs = Presentation(template_path)
    client_name = deal["name"]

    replacements = {
        "[Placeholder DD/Month/YYYY]": TODAY_STR,
        "DD/Month/YYYY": TODAY_STR,
        "Placeholder DD/Month/YYYY": TODAY_STR,
        "BNI Life": client_name,
        "BNI LIfe": client_name,
        "BNI LIFE": client_name.upper(),
    }

    for slide in prs.slides:
        for shape in slide.shapes:
            if shape.has_text_frame:
                for para in shape.text_frame.paragraphs:
                    for run in para.runs:
                        for old, new in replacements.items():
                            if old in run.text:
                                run.text = run.text.replace(old, new)
            if shape.has_table:
                for row in shape.table.rows:
                    for cell in row.cells:
                        for para in cell.text_frame.paragraphs:
                            for run in para.runs:
                                for old, new in replacements.items():
                                    if old in run.text:
                                        run.text = run.text.replace(old, new)

    safe_name = re.sub(r'[^\w\s-]', '', client_name).strip().replace(' ', '_')
    out_path = os.path.join(OUTPUT_DIR, safe_name, f"Sembuh AI - {client_name} - Proposal v1.pptx")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    prs.save(out_path)
    print(f"    Proposal:  {out_path}")
    return out_path


# ══════════════════════════════════════════════════════════════
# GOOGLE DRIVE UPLOAD
# ══════════════════════════════════════════════════════════════

def _get_google_tools():
    sys.path.insert(0, _BASE_DIR)
    from google_tools import drive_create_file, drive_upload_file, gmail_create_draft
    return drive_create_file, drive_upload_file, gmail_create_draft


def upload_to_drive(files, client_name):
    drive_create_file, drive_upload_file, _ = _get_google_tools()

    result = drive_create_file(title=client_name, mime_type="folder", folder_id=GOOGLE_FOLDER_ID)
    folder_match = re.search(r"id: ([a-zA-Z0-9_-]+)", result)
    client_folder_id = folder_match.group(1) if folder_match else GOOGLE_FOLDER_ID

    subfolder_map = {"Proposals": "proposal", "Quotations": "quotation", "Pricing": "pricing"}
    for subfolder, keyword in subfolder_map.items():
        sub_result = drive_create_file(title=subfolder, mime_type="folder", folder_id=client_folder_id)
        sub_match = re.search(r"id: ([a-zA-Z0-9_-]+)", sub_result)
        sub_id = sub_match.group(1) if sub_match else client_folder_id
        for f in files:
            if keyword in os.path.basename(f).lower():
                drive_upload_file(f, folder_id=sub_id)
                print(f"    Uploaded {os.path.basename(f)} -> {subfolder}/")

    drive_url = f"https://drive.google.com/drive/folders/{client_folder_id}"
    return client_folder_id, drive_url


# ══════════════════════════════════════════════════════════════
# GMAIL DRAFT
# ══════════════════════════════════════════════════════════════

def create_email_draft(deal, files):
    _, _, gmail_create_draft = _get_google_tools()

    client_name = deal["name"]
    products_str = ", ".join(deal["products"])

    subject = f"Sembuh AI — Proposal for {client_name} ({products_str})"
    body = f"""Dear {client_name} Team,

Thank you for your interest in Sembuh AI's solutions. Please find attached our proposal and quotation.

Proposal Summary:
- Client: {client_name}
- Solutions: {products_str}
- Stage: {deal['stage']}
- Monthly Claim Volume: {int(deal['monthly_claim_vol']):,}
- Members Covered: {int(deal['members_covered']):,}

Attached:
1. Proposal — Solutions overview and implementation plan
2. Quotation — Detailed pricing breakdown

Implementation Plan:
- Week 1: Setup and requirements alignment
- Week 2: API integration with core system
- Week 3: Testing and UAT
- Week 4: Go-live and monitoring

Best regards,
Sembuh AI Sales Team
www.sembuh.ai

---
Auto-generated draft — review before sending.
Generated on {TODAY_STR}
"""

    result = gmail_create_draft(
        to="sales@sembuh.ai", subject=subject, body=body,
        attachments=[f for f in files if "pricing" not in os.path.basename(f).lower()],
    )
    print(f"    Gmail draft: {result}")
    # Extract draft ID for Gmail link
    draft_id_match = re.search(r"ID:\s*(\S+)", result)
    draft_id = draft_id_match.group(1) if draft_id_match else None
    return draft_id


# ══════════════════════════════════════════════════════════════
# SLACK NOTIFICATION (per Slack-Report-Spec.docx)
# ══════════════════════════════════════════════════════════════

def _slack_post(channel, text, blocks):
    """Post message to Slack channel."""
    resp = requests.post(f"{SLACK_API_URL}/chat.postMessage", json={
        "channel": channel, "text": text, "blocks": blocks,
    }, headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"})
    data = resp.json()
    if data.get("ok"):
        print(f"  Slack sent to {channel}")
    else:
        print(f"  Slack error ({channel}): {data.get('error')}")
    return data


def _health_emoji(score):
    if score >= 80:
        return ":large_green_circle:"
    if score >= 50:
        return ":warning:"
    if score > 0:
        return ":red_circle:"
    return ":white_circle:"


def _health_label(score):
    if score >= 80:
        return "Healthy"
    if score >= 50:
        return "At Risk"
    if score > 0:
        return "Critical"
    return "Lost"


def slack_managerial_report(deals, generated_files=None, drive_links=None, draft_links=None):
    """Managerial report → #bod-updates style (Section 4.1 of spec)."""
    drive_links = drive_links or {}
    draft_links = draft_links or {}

    # Compute health for all deals
    scored = [(d, *compute_health_score(d)) for d in deals]

    active = [d for d in deals if d["stage"] not in ("Lost",)]
    total_value = sum(d["deal_value"] for d in active)
    weighted = sum(d["deal_value"] * STAGE_WEIGHTS.get(d["stage"], 0.1) for d in active)
    prob_pct = int((weighted / total_value * 100) if total_value else 0)

    # Health distribution
    healthy = [(d, s) for d, s, st, r in scored if st == "Healthy"]
    at_risk = [(d, s) for d, s, st, r in scored if st == "At Risk"]
    critical = [(d, s) for d, s, st, r in scored if st == "Critical"]
    lost = [(d, s) for d, s, st, r in scored if st == "Lost"]

    healthy_val = sum(d["deal_value"] for d, _ in healthy)
    at_risk_val = sum(d["deal_value"] for d, _ in at_risk)
    critical_val = sum(d["deal_value"] for d, _ in critical)
    lost_val = sum(d["deal_value"] for d, _ in lost)

    # Build message
    lines = [
        f":bar_chart:  *Weekly Pipeline Report — {TODAY_STR}*",
        "",
        "*Pipeline Overview*",
        f"│  Total Active Deals:   {len(active)}",
        f"│  Total Pipeline Value:  ${total_value:,.0f}",
        f"│  Weighted Forecast:     ${weighted:,.0f} ({prob_pct}% probability)",
        "",
        "*Health Distribution*",
        f":large_green_circle:  Healthy:    {len(healthy)} deal{'s' if len(healthy) != 1 else ''}  (${healthy_val:,.0f})",
        f":warning:  At Risk:    {len(at_risk)} deal{'s' if len(at_risk) != 1 else ''}  (${at_risk_val:,.0f})",
        f":red_circle:  Critical:   {len(critical)} deal{'s' if len(critical) != 1 else ''}  (${critical_val:,.0f})",
        f":white_circle:  Lost:       {len(lost)} deal{'s' if len(lost) != 1 else ''}  (${lost_val:,.0f})",
    ]

    # Deals requiring attention (score < 80, sorted by score asc)
    attention = [(d, s, r) for d, s, st, r in scored if st in ("At Risk", "Critical")]
    attention.sort(key=lambda x: x[1])
    if attention:
        lines.append("")
        lines.append(":rotating_light:  *Deals Requiring Attention*")
        for i, (d, score, reasons) in enumerate(attention[:5]):
            emoji = _health_emoji(score)
            reason_str = "; ".join(reasons)
            owner_str = f"Owner: {d['owner']}" if d["owner"] else "Owner: Unassigned"
            lines.append(f"{i+1}. *{d['name']}* (${d['deal_value']:,.0f})")
            lines.append(f"   Score: {score}/100 {emoji} | {d['stage']} | {owner_str}")
            lines.append(f"   :arrow_right: {reason_str}")

    # Positive signals
    positive = [(d, s) for d, s, st, r in scored if st == "Healthy" and d["stage"] not in ("Won", "Implementation Done")]
    if positive:
        lines.append("")
        lines.append(":white_check_mark:  *Positive Signals*")
        for d, score in positive:
            lines.append(f"• {d['name']} — {d['stage']}, on track (score {score}/100)")

    # AI Insight
    stuck_stages = {}
    for d in active:
        stuck_stages[d["stage"]] = stuck_stages.get(d["stage"], 0) + 1
    most_stuck = max(stuck_stages, key=stuck_stages.get) if stuck_stages else None
    if most_stuck and stuck_stages[most_stuck] > 1:
        lines.append("")
        lines.append(f":bulb: *AI Insight:* {stuck_stages[most_stuck]} deals at _{most_stuck}_ stage. Consider accelerating follow-up cadence.")

    # Generated files section
    if generated_files:
        lines.append("")
        lines.append(":page_facing_up: *Documents Generated*")
        for deal, files in zip(deals, generated_files):
            if files:
                file_names = ", ".join(f"`{os.path.basename(f)}`" for f in files)
                lines.append(f"• {deal['name']}: {file_names}")
                link_parts = []
                if deal["name"] in drive_links:
                    link_parts.append(f"<{drive_links[deal['name']]}|:file_folder: Google Drive>")
                if deal["name"] in draft_links:
                    link_parts.append(f"<{draft_links[deal['name']]}|:envelope: Gmail Draft>")
                if link_parts:
                    lines.append(f"   {' · '.join(link_parts)}")

    lines.append("")
    lines.append(f"<@{SLACK_NOTIFY_USER}>")
    lines.append("_Powered by Sales Intelligence Engine_")

    text = "\n".join(lines)
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
    return _slack_post(SLACK_CHANNEL_ID, f"<@{SLACK_NOTIFY_USER}> Weekly Pipeline Report — {TODAY_STR}", blocks)


def slack_staff_report(deals, am_name=None, drive_links=None, draft_links=None):
    """Staff report → #am-{name} style (Section 5.1 of spec). Sent to main channel for demo."""
    drive_links = drive_links or {}
    draft_links = draft_links or {}

    if am_name:
        my_deals = [d for d in deals if am_name.lower() in (d.get("owner") or "").lower()]
    else:
        my_deals = deals

    if not my_deals:
        return

    display_name = am_name or "Team"
    day_name = TODAY.strftime("%A, %B %d, %Y")

    lines = [
        f":wave:  Good morning, {display_name}! Here's your deal briefing.",
        f"*{day_name}*",
        "",
        f"*Your Active Deals ({len(my_deals)})*",
        "───────────────────────────",
    ]

    total_pipeline = 0
    urgent_count = 0
    warning_count = 0
    top_priority = None

    for d in my_deals:
        score, status, reasons = compute_health_score(d)
        emoji = _health_emoji(score)
        total_pipeline += d["deal_value"]
        reason_line = "; ".join(reasons) if reasons else "On track"

        if score < 50:
            urgent_count += 1
            if not top_priority:
                top_priority = d["name"]
        elif score < 80:
            warning_count += 1
            if not top_priority:
                top_priority = d["name"]

        lines.append(f"{emoji}  *{d['name']}*")
        lines.append(f"   ${d['deal_value']:,.0f}  |  {d['stage']}  |  Score: {score}/100")

        if status == "Critical":
            lines.append(f"   :rotating_light: {reason_line}")
        elif status == "At Risk":
            lines.append(f"   :warning: {reason_line}")
        else:
            lines.append(f"   :white_check_mark: {reason_line}")

        # Action item
        if score < 50:
            lines.append(f"   :arrow_right: Urgent: Address immediately")
        elif score < 80:
            lines.append(f"   :arrow_right: Follow up this week")
        else:
            lines.append(f"   :arrow_right: Continue current engagement")

        # Links
        link_parts = []
        if d["name"] in drive_links:
            link_parts.append(f"<{drive_links[d['name']]}|:file_folder: Drive>")
        if d["name"] in draft_links:
            link_parts.append(f"<{draft_links[d['name']]}|:envelope: Draft>")
        if link_parts:
            lines.append(f"   {' · '.join(link_parts)}")
        lines.append("")

    lines.append("───────────────────────────")
    lines.append(f"*Summary:* {len(my_deals)} deals | ${total_pipeline:,.0f} pipeline | {urgent_count} urgent, {warning_count} warning")
    if top_priority:
        lines.append(f"*Top priority today:* {top_priority}")

    lines.append("")
    lines.append(f"<@{SLACK_NOTIFY_USER}>")

    text = "\n".join(lines)
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
    return _slack_post(SLACK_CHANNEL_ID, f"<@{SLACK_NOTIFY_USER}> Daily briefing for {display_name}", blocks)


def slack_critical_alert(deal, score, reasons):
    """Real-time critical alert (Section 4.2 of spec)."""

    reason_str = "; ".join(reasons)
    owner_mention = f"<@{SLACK_NOTIFY_USER}>" if deal.get("owner") else "Unassigned"

    text = "\n".join([
        f":rotating_light:  *Critical Deal Alert*",
        "",
        f"*Deal:* {deal['name']}",
        f"*Value:* ${deal['deal_value']:,.0f}  |  *Stage:* {deal['stage']}",
        f"*Health:* Score dropped to {score}/100 :red_circle:",
        "",
        f"*What happened:* {reason_str}",
        f"*Assigned to:* {owner_mention} ({deal.get('owner') or 'Unassigned'})",
        f"*Recommended:* Review deal and take immediate action.",
        "",
        "_Powered by Sales Intelligence Engine_",
    ])

    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
    return _slack_post(SLACK_CHANNEL_ID, f":rotating_light: Critical: {deal['name']} — {score}/100", blocks)


def slack_deal_activity(deal, event_summary, agent_actions, next_step, drive_url=None, draft_url=None):
    """Real-time deal activity alert (Section 5.2 of spec)."""

    actions_lines = "\n".join(f":white_check_mark: {a}" for a in agent_actions)

    link_lines = []
    if drive_url:
        link_lines.append(f":file_folder: <{drive_url}|Open Google Drive folder>")
    if draft_url:
        link_lines.append(f":envelope: <{draft_url}|Open Gmail draft>")

    text = "\n".join([
        f":incoming_envelope:  *New activity on your deal*",
        "",
        f"*Deal:* {deal['name']} (${deal['deal_value']:,.0f})",
        f"*Event:* {event_summary}",
        "",
        "*Agent actions completed:*",
        actions_lines,
        "",
        *(["*Quick Links:*"] + link_lines + [""] if link_lines else []),
        "*Your next step:*",
        f":arrow_right: {next_step}",
        "",
        f"<@{SLACK_NOTIFY_USER}>",
    ])

    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]
    return _slack_post(SLACK_CHANNEL_ID, f":incoming_envelope: Activity: {deal['name']}", blocks)


# ══════════════════════════════════════════════════════════════
# DEAL HEALTH SCORING
# ══════════════════════════════════════════════════════════════

def compute_health_score(deal):
    score = 100
    reasons = []
    stage = deal["stage"]

    if stage in ("Lost",):
        return 0, "Lost", ["Deal marked as Lost."]
    if stage in ("Won", "Implementation Done", "[Won] Waiting Signature"):
        return 100, "Healthy", ["Deal won."]

    # No owner
    if not deal["owner"]:
        score -= 15
        reasons.append("No owner assigned")

    # No deal value
    if deal["deal_value"] == 0:
        score -= 10
        reasons.append("Deal value empty")

    # Close date passed
    if deal["close_date"]:
        try:
            close = datetime.date.fromisoformat(deal["close_date"])
            if close < TODAY:
                score -= 20
                reasons.append(f"Close date {deal['close_date']} has passed")
        except ValueError:
            pass

    # No proposal for advanced stages
    advanced = ("Solutioning", "Waiting Confirmation", "[Won] Waiting Signature")
    if stage in advanced and not deal["proposal_date"]:
        score -= 10
        reasons.append(f"Stage is {stage} but no proposal sent")

    # High excess rate
    if deal["incurred"] > 0:
        excess_rate = deal["excess"] / deal["incurred"]
        if excess_rate > 0.30:
            score -= 10
            reasons.append(f"High excess rate ({excess_rate:.0%})")

    score = max(score, 0)
    if score >= 80:
        status = "Healthy"
    elif score >= 50:
        status = "At Risk"
    else:
        status = "Critical"

    return score, status, reasons


# ══════════════════════════════════════════════════════════════
# MoM EXTRACTION
# ══════════════════════════════════════════════════════════════

def extract_docx_text(filepath):
    from docx import Document
    doc = Document(filepath)
    parts = []
    for para in doc.paragraphs:
        if para.text.strip():
            parts.append(para.text)
    for i, table in enumerate(doc.tables):
        parts.append(f"\n=== TABLE {i + 1} ===")
        for row in table.rows:
            parts.append(" | ".join(cell.text.strip() for cell in row.cells))
    return "\n".join(parts)


# ══════════════════════════════════════════════════════════════
# CLI COMMANDS
# ══════════════════════════════════════════════════════════════

def cmd_generate(args):
    """Fetch deals from Monday.com, generate all documents, upload, email, notify."""
    print("=" * 60)
    print("Sales Intelligence — Document Generator")
    print("=" * 60)

    # 1. Fetch deals
    print("\n[1/5] Fetching deals from Monday.com...")
    boards = fetch_boards()
    deals_board = find_deals_board(boards)
    if not deals_board:
        print("No board found."); sys.exit(1)

    print(f"  Board: {deals_board['name']} (id: {deals_board['id']})")
    raw_deals = fetch_deals(deals_board["id"])
    deals = [parse_deal(d) for d in raw_deals]

    # Filter by --deal if specified
    if args.deal:
        deals = [d for d in deals if args.deal.lower() in d["name"].lower()]
        if not deals:
            print(f"  No deal matching '{args.deal}'"); sys.exit(1)

    print(f"  Processing {len(deals)} deals")

    # 2. Generate docs
    print("\n[2/5] Generating documents...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_files = []
    for i, deal in enumerate(deals):
        print(f"\n  [{deal['name']}] ({deal['stage']})")
        files = [
            generate_quotation(deal, seq_num=i + 1),
            generate_pricing_internal(deal, deals),
            generate_proposal(deal),
        ]
        all_files.append(files)

    # 3. Upload to Google Drive
    print("\n[3/5] Uploading to Google Drive...")
    drive_links = {}
    for deal, files in zip(deals, all_files):
        try:
            _, drive_url = upload_to_drive(files, deal["name"])
            drive_links[deal["name"]] = drive_url
        except Exception as e:
            print(f"    Drive error ({deal['name']}): {e}")

    # 4. Gmail drafts
    print("\n[4/5] Creating Gmail drafts...")
    draft_links = {}
    for deal, files in zip(deals, all_files):
        if deal["stage"] == "Lost":
            print(f"    Skip {deal['name']} (Lost)"); continue
        try:
            draft_id = create_email_draft(deal, files)
            if draft_id:
                draft_links[deal["name"]] = f"https://mail.google.com/mail/u/0/#drafts?compose={draft_id}"
        except Exception as e:
            print(f"    Gmail error ({deal['name']}): {e}")

    # 5. Slack — managerial + staff reports
    print("\n[5/5] Sending Slack reports...")
    try:
        slack_managerial_report(deals, all_files, drive_links=drive_links, draft_links=draft_links)
        owners = set(d["owner"] for d in deals if d["owner"])
        for owner in owners:
            slack_staff_report(deals, am_name=owner, drive_links=drive_links, draft_links=draft_links)
        if not owners:
            slack_staff_report(deals, drive_links=drive_links, draft_links=draft_links)
        for deal in deals:
            score, status, reasons = compute_health_score(deal)
            if score < 50 and status == "Critical":
                slack_critical_alert(deal, score, reasons)
    except Exception as e:
        print(f"    Slack error: {e}")

    print(f"\nDone. Output: {OUTPUT_DIR}")


def cmd_mom(args):
    """Extract MoM from docx and update Monday.com CRM via MCP."""
    if not os.path.exists(args.docx_path):
        print(f"File not found: {args.docx_path}"); sys.exit(1)

    print(f"Extracting MoM: {args.docx_path}")
    mom_text = extract_docx_text(args.docx_path)
    print(f"  {len(mom_text)} chars extracted")

    if args.dry_run:
        print("\n=== DRY RUN ===\n")
        print(mom_text)
        return

    async def _run():
        client = MondayMCPClient()
        print("Connecting to Monday MCP...", file=sys.stderr)
        await client.connect()

        system = (
            f"You are a Sales Intelligence CRM updater. "
            f"Workspace ID: {MONDAY_WORKSPACE_ID}. "
            f"Always pass workspace_ids=[{MONDAY_WORKSPACE_ID}] when listing boards. "
            "Analyze MoM data, find matching deal, update CRM fields, add comment summary."
        )

        prompt = f"""\
Here is the extracted Minutes of Meeting (MoM). Update Monday.com CRM accordingly.

--- MoM ---
{mom_text}
--- End ---

Board & Column Reference (Deals board ID from workspace):
  - name: Deal name
  - deal_stage: Status (Waiting Confirmation, Won, Lost, Solutioning, First Meeting Done, Piloting)
  - deal_value: Deal Value (numbers)
  - deal_expected_close_date: Close Date (date format: {{"date":"YYYY-MM-DD"}})
  - deal_close_probability: Close Probability (numbers)

Instructions:
1. List boards in workspace to find Deals board.
2. Search for the deal matching this MoM client.
3. Update relevant columns from MoM data.
4. Add a comment summarizing key MoM points (meeting date, attendees, summary, action items, next steps).
   Keep comment under 2000 chars, plain ASCII only.
"""

        try:
            result, _ = await client.chat(prompt, [], system=system)
            print(f"\n{result}")
        finally:
            await client.cleanup()

    asyncio.run(_run())


def cmd_health(args):
    """Run deal health scoring on all deals."""
    print("Sales Intelligence — Deal Health Monitor\n")

    boards = fetch_boards()
    deals_board = find_deals_board(boards)
    raw_deals = fetch_deals(deals_board["id"])
    deals = [parse_deal(d) for d in raw_deals]

    print(f"{'Deal':<40} {'Score':>5} {'Status':<10} Reasons")
    print("-" * 100)

    critical_deals = []
    for deal in deals:
        score, status, reasons = compute_health_score(deal)
        reason_str = "; ".join(reasons) if reasons else "On track"
        indicator = {"Healthy": "G", "At Risk": "A", "Critical": "R", "Lost": "X"}.get(status, "?")
        print(f"[{indicator}] {deal['name']:<37} {score:>5} {status:<10} {reason_str}")
        if score < 50:
            critical_deals.append((deal, score, reasons))

    # Slack alerts for critical deals
    if critical_deals and SLACK_BOT_TOKEN:
        print(f"\nSending Slack alerts for {len(critical_deals)} critical deals...")
        for deal, score, reasons in critical_deals:
            slack_critical_alert(deal, score, reasons)

    # Full managerial report if requested
    if SLACK_BOT_TOKEN:
        print("\nSending managerial health report...")
        slack_managerial_report(deals)


def cmd_pipeline(args):
    """Full pipeline: MoM → CRM update → generate docs → Drive → Gmail → Slack. One command."""

    print("=" * 60)
    print("Sales Intelligence — Full Pipeline")
    print("=" * 60)

    docx_path = args.docx_path

    # ── STEP 1: Extract MoM & update Monday.com CRM ──────────
    print(f"\n{'='*60}")
    print("STEP 1/3 — MoM Extraction & CRM Update")
    print("=" * 60)

    if not os.path.exists(docx_path):
        print(f"  MoM file not found: {docx_path}")
        print("  Skipping MoM step. Proceeding with existing CRM data.\n")
        mom_text = None
    else:
        print(f"  Extracting: {docx_path}")
        mom_text = extract_docx_text(docx_path)
        print(f"  {len(mom_text)} chars extracted")

        async def _update_crm():
            client = MondayMCPClient()
            print("  Connecting to Monday MCP...", file=sys.stderr)
            await client.connect()

            system = (
                f"You are a Sales Intelligence CRM updater. "
                f"Workspace ID: {MONDAY_WORKSPACE_ID}. "
                f"Always pass workspace_ids=[{MONDAY_WORKSPACE_ID}] when listing boards. "
                "Analyze MoM data, find matching deal, update CRM fields, add comment summary."
            )

            prompt = f"""\
Here is the extracted Minutes of Meeting (MoM). Update Monday.com CRM accordingly.

--- MoM ---
{mom_text}
--- End ---

Board & Column Reference (Deals board ID from workspace):
  - name: Deal name
  - deal_stage: Status (Waiting Confirmation, Won, Lost, Solutioning, First Meeting Done, Piloting)
  - deal_value: Deal Value (numbers)
  - deal_expected_close_date: Close Date (date format: {{"date":"YYYY-MM-DD"}})
  - deal_close_probability: Close Probability (numbers)

Instructions:
1. List boards in workspace to find Deals board.
2. Search for the deal matching this MoM client.
3. Update relevant columns from MoM data.
4. Add a comment summarizing key MoM points (meeting date, attendees, summary, action items, next steps).
   Keep comment under 2000 chars, plain ASCII only.
"""
            try:
                result, _ = await client.chat(prompt, [], system=system)
                print(f"\n  CRM Update Result:\n{result}")
            finally:
                await client.cleanup()

        try:
            asyncio.run(_update_crm())
            print("\n  CRM update complete.")
        except Exception as e:
            print(f"\n  CRM update error: {e}")
            print("  Continuing with document generation...\n")

    # ── STEP 2: Generate Documents (Point 2 & 3) ─────────────
    print(f"\n{'='*60}")
    print("STEP 2/3 — Document Generation")
    print("=" * 60)

    print("\n  Fetching deals from Monday.com...")
    boards = fetch_boards()
    deals_board = find_deals_board(boards)
    if not deals_board:
        print("  No board found."); sys.exit(1)

    print(f"  Board: {deals_board['name']} (id: {deals_board['id']})")
    raw_deals = fetch_deals(deals_board["id"])
    deals = [parse_deal(d) for d in raw_deals]

    if args.deal:
        deals = [d for d in deals if args.deal.lower() in d["name"].lower()]
        if not deals:
            print(f"  No deal matching '{args.deal}'"); sys.exit(1)

    print(f"  Processing {len(deals)} deals\n")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_files = []
    for i, deal in enumerate(deals):
        print(f"  [{deal['name']}] ({deal['stage']})")
        files = [
            generate_quotation(deal, seq_num=i + 1),
            generate_pricing_internal(deal, deals),
            generate_proposal(deal),
        ]
        all_files.append(files)

    # Upload to Google Drive
    print("\n  Uploading to Google Drive...")
    drive_links = {}  # deal_name -> drive_url
    for deal, files in zip(deals, all_files):
        try:
            _, drive_url = upload_to_drive(files, deal["name"])
            drive_links[deal["name"]] = drive_url
        except Exception as e:
            print(f"    Drive error ({deal['name']}): {e}")

    # Gmail drafts
    print("\n  Creating Gmail drafts...")
    draft_links = {}  # deal_name -> gmail_url
    for deal, files in zip(deals, all_files):
        if deal["stage"] == "Lost":
            print(f"    Skip {deal['name']} (Lost)"); continue
        try:
            draft_id = create_email_draft(deal, files)
            if draft_id:
                draft_links[deal["name"]] = f"https://mail.google.com/mail/u/0/#drafts?compose={draft_id}"
        except Exception as e:
            print(f"    Gmail error ({deal['name']}): {e}")

    # ── STEP 3: Health Check & Slack Notification ─────────────
    print(f"\n{'='*60}")
    print("STEP 3/3 — Health Check & Notifications")
    print("=" * 60)

    # Health scores
    print("\n  Deal Health Scores:")
    print(f"  {'Deal':<35} {'Score':>5} {'Status':<10} Reasons")
    print(f"  {'-'*85}")
    critical_deals = []
    for deal in deals:
        score, status, reasons = compute_health_score(deal)
        reason_str = "; ".join(reasons) if reasons else "On track"
        ind = {"Healthy": "G", "At Risk": "A", "Critical": "R", "Lost": "X"}.get(status, "?")
        print(f"  [{ind}] {deal['name']:<32} {score:>5} {status:<10} {reason_str}")
        if score < 50:
            critical_deals.append((deal, score, reasons))

    # Slack reports (per spec)
    print("\n  Sending Slack reports...")

    # 1. Managerial report (#bod-updates style)
    slack_managerial_report(deals, all_files, drive_links=drive_links, draft_links=draft_links)

    # 2. Staff reports per AM
    owners = set(d["owner"] for d in deals if d["owner"])
    for owner in owners:
        slack_staff_report(deals, am_name=owner, drive_links=drive_links, draft_links=draft_links)
    if not owners:
        slack_staff_report(deals, drive_links=drive_links, draft_links=draft_links)

    # 3. Critical alerts
    for deal, score, reasons in critical_deals:
        slack_critical_alert(deal, score, reasons)

    # 4. Deal activity alert (for MoM-processed deal)
    if mom_text:
        mom_deal = deals[0] if len(deals) == 1 else None
        if mom_deal:
            deal_drive = drive_links.get(mom_deal["name"])
            deal_draft = draft_links.get(mom_deal["name"])
            slack_deal_activity(
                mom_deal,
                event_summary=f"MoM processed from `{os.path.basename(docx_path)}`",
                agent_actions=[
                    "MoM extracted and parsed",
                    "CRM updated with meeting notes",
                    f"Documents generated (Quotation + Proposal + Pricing)",
                    "Gmail draft created",
                    "Files uploaded to Google Drive",
                ],
                next_step="Review Gmail draft and send to client",
                drive_url=deal_drive,
                draft_url=deal_draft,
            )

    # Summary
    print(f"\n{'='*60}")
    print("PIPELINE COMPLETE")
    print(f"  MoM:       {'Processed' if mom_text else 'Skipped'}")
    print(f"  Deals:     {len(deals)}")
    print(f"  Files:     {sum(len(f) for f in all_files)}")
    print(f"  Output:    {OUTPUT_DIR}")
    print(f"  Slack:     {SLACK_CHANNEL_ID}")
    print("=" * 60)


def cmd_interactive(args):
    """Interactive Monday.com assistant."""
    async def _run():
        client = MondayMCPClient()
        print("Monday.com AI Assistant")
        print("Connecting...", file=sys.stderr)
        await client.connect()
        print("Ready. Type 'quit' to exit.\n")

        history = []
        while True:
            try:
                prompt = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nBye!"); break
            if not prompt:
                continue
            if prompt.lower() in ("quit", "exit", "q"):
                print("Bye!"); break

            try:
                text, history = await client.chat(prompt, history)
                print(f"\n{text}\n")
            except Exception as e:
                print(f"\nError: {e}\n", file=sys.stderr)

        await client.cleanup()

    asyncio.run(_run())


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Sales Intelligence — Unified Automation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python app.py pipeline                          Full pipeline: MoM → CRM → docs → Drive → Gmail → Slack
  python app.py pipeline --deal "Sentosa"         Pipeline for one deal
  python app.py pipeline --mom MoM.docx           Pipeline with specific MoM file
  python app.py generate                          Generate all deal documents
  python app.py generate --deal "Sentosa"         Generate for one deal
  python app.py mom MoM.docx                      Extract MoM and update CRM
  python app.py mom MoM.docx --dry-run            Preview MoM extraction only
  python app.py health                            Run deal health scoring
  python app.py interactive                       Interactive Monday.com chat
""",
    )
    sub = parser.add_subparsers(dest="command")

    # pipeline (the one-shot command)
    pipe = sub.add_parser("pipeline", help="Full pipeline: MoM → CRM update → generate docs → Drive → Gmail → Slack")
    pipe.add_argument("--mom", dest="docx_path",
                      default=os.path.join(TEMPLATE_DIR, "MoM-Sentosa-Health-PoC-Review.docx"),
                      help="Path to MoM .docx file (default: sample MoM)")
    pipe.add_argument("--deal", default="", help="Filter by deal name (substring match)")

    # generate
    gen = sub.add_parser("generate", help="Generate documents from Monday.com deals")
    gen.add_argument("--deal", default="", help="Filter by deal name (substring match)")

    # mom
    mom = sub.add_parser("mom", help="Extract MoM and update Monday.com CRM")
    mom.add_argument("docx_path", nargs="?",
                     default=os.path.join(TEMPLATE_DIR, "MoM-Sentosa-Health-PoC-Review.docx"),
                     help="Path to MoM .docx file")
    mom.add_argument("--dry-run", action="store_true", help="Preview only, don't update CRM")

    # health
    sub.add_parser("health", help="Run deal health scoring")

    # interactive
    sub.add_parser("interactive", help="Interactive Monday.com assistant")

    args = parser.parse_args()

    if args.command == "pipeline":
        cmd_pipeline(args)
    elif args.command == "generate":
        cmd_generate(args)
    elif args.command == "mom":
        cmd_mom(args)
    elif args.command == "health":
        cmd_health(args)
    elif args.command == "interactive":
        cmd_interactive(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
