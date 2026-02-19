import requests
import json
import time
import logging
import sys
import os
from datetime import date, datetime
import gspread
from google.oauth2 import service_account
import pandas as pd
import pytz
from dotenv import load_dotenv
from requests.exceptions import RequestException

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s",
    handlers=[logging.StreamHandler(open(sys.stdout.fileno(), mode='w', encoding='utf-8', closefd=False))]
)
log = logging.getLogger()

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

COMPANIES = {
    1: "Zipper",
    3: "Metal Trims",
}

AGEING_SLOT  = "181_365"   # 181-365 Days
DISPLAY_TYPE = "all"       # All (Qty & Value)

SHEET_KEY = "1j37Y6g3pnMWtwe2fjTe1JTT32aRLS0Z1YPjl3v657Cc"

WORKSHEET_MAP = {
    1: "Zipper_Products_Raw",
    3: "Mt_Products_Raw",
}

today = date.today()
session = requests.Session()
USER_ID = None

# ========= FISCAL YEAR HELPER ==========
def get_fiscal_year_str(ref_date=None):
    """Returns fiscal year string like '2025-26' for the year containing ref_date.
    Fiscal year runs April 1 → March 31."""
    if ref_date is None:
        ref_date = date.today()
    if ref_date.month >= 4:
        fy_start_year = ref_date.year
    else:
        fy_start_year = ref_date.year - 1
    fy_end_year = fy_start_year + 1
    return f"{fy_start_year}-{str(fy_end_year)[2:]}"

# ========= RETRY WRAPPER ==========
def retry_request(method, url, max_retries=3, backoff=3, **kwargs):
    for attempt in range(1, max_retries + 1):
        try:
            r = method(url, **kwargs)
            r.raise_for_status()
            return r
        except RequestException as e:
            log.warning(f"⚠️  Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                log.info(f"⏳ Retrying in {backoff}s...")
                time.sleep(backoff)
            else:
                log.error("❌ All retry attempts failed.")
                raise

# ========= LOGIN ==========
def login():
    global USER_ID
    payload = {
        "jsonrpc": "2.0",
        "params": {"db": DB, "login": USERNAME, "password": PASSWORD},
    }
    r = retry_request(
        session.post,
        f"{ODOO_URL}/web/session/authenticate",
        json=payload,
    )
    result = r.json().get("result")
    if result and "uid" in result:
        USER_ID = result["uid"]
        log.info(f"✅ Logged in (uid={USER_ID})")
        return result
    raise Exception("❌ Login failed")

# ========= SWITCH COMPANY ==========
def switch_company(company_id):
    if USER_ID is None:
        raise Exception("User not logged in yet")
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "res.users",
            "method": "write",
            "args": [[USER_ID], {"company_id": company_id}],
            "kwargs": {
                "context": {
                    "allowed_company_ids": [company_id],
                    "company_id": company_id,
                }
            },
        },
    }
    r = retry_request(
        session.post,
        f"{ODOO_URL}/web/dataset/call_kw",
        json=payload,
    )
    if "error" in r.json():
        log.error(f"❌ Failed to switch company {company_id}: {r.json()['error']}")
        return False
    log.info(f"🔄 Session switched to company {company_id}")
    return True

# ========= FETCH AGEING DATA ==========
def fetch_ageing_data(company_id, cname):
    """Fetches ageing summary report filtered by:
      - Company: company_id
      - Ageing Slot: 181-365 Days  (AGEING_SLOT = "181_365")
      - Display: All (Qty & Value) (DISPLAY_TYPE = "all")
      - Fiscal Year: current FY    (e.g. "2025-26")
    """
    fiscal_year = get_fiscal_year_str()
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "rm.ageing.summary.report",
            "method": "retrive_ageing_by_item_cat_data",
            "args": [str(company_id), AGEING_SLOT, DISPLAY_TYPE, fiscal_year],
            "kwargs": {
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": USER_ID,
                    "allowed_company_ids": list(COMPANIES.keys()),
                }
            },
        },
    }
    r = retry_request(
        session.post,
        f"{ODOO_URL}/web/dataset/call_kw/rm.ageing.summary.report/retrive_ageing_by_item_cat_data",
        json=payload,
    )
    result = r.json().get("result", {})
    if not result.get("success"):
        log.warning(f"⚠️  {cname}: API returned no data — {result.get('message', 'unknown')}")
        return {}
    log.info(
        f"📊 {cname}: {len(result.get('item_categories', []))} categories "
        f"(FY={fiscal_year}, slot={AGEING_SLOT}, display={DISPLAY_TYPE})"
    )
    return result

# ========= TRANSFORM TO WIDE FORMAT ==========
def transform_to_wide(result, cname):
    """
    Builds wide format from the ageing summary response:
      Row 1 (header): Item Category | Feb 2026 | Feb 2026 | Jan 2026 | Jan 2026 | ...
      Row 2 (sub-hdr): (empty)      | Value    | Qty      | Value    | Qty      | ...
      Data rows: one row per item_category, values from slot_value / slot_qty
    Months are in the order returned by the API (newest first).
    """
    data          = result.get("data", {})
    months        = result.get("months", [])          # e.g. ["2026-02-18", "2026-01-31", ...]
    month_display = result.get("month_display", [])   # e.g. ["Feb 2026", "Jan 2026", ...]
    categories    = result.get("item_categories", [])

    if not data or not months or not categories:
        log.warning(f"No data to transform for {cname}")
        return [], [], []

    # Build header rows
    header1 = ["Item Category"]
    header2 = [""]
    for ml in month_display:
        header1 += [ml, ml]
        header2 += ["Value", "Qty"]

    # Build data rows
    data_rows = []
    for cat in categories:
        row = [cat]
        month_data = data.get(cat, {}).get("months", {})
        for m in months:
            rec = month_data.get(m, {})
            row += [
                rec.get("slot_value", 0.0),
                rec.get("slot_qty",   0.0),
            ]
        data_rows.append(row)

    log.info(f"📐 {cname}: {len(categories)} categories × {len(months)} months → wide table ready")
    return header1, header2, data_rows

# ========= PASTE TO GOOGLE SHEETS ==========
def paste_to_sheet(header1, header2, data_rows, worksheet_name, cname):
    if not data_rows:
        log.warning(f"⚠️  {cname}: No data rows. Skipping {worksheet_name}.")
        return

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = service_account.Credentials.from_service_account_file(
        "service_account.json", scopes=scope
    )
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_KEY)
    worksheet = sheet.worksheet(worksheet_name)

    # Clear existing content
    worksheet.batch_clear(["A:ZZ"])

    # Write header row 1 (month labels)
    worksheet.update("A1", [header1], value_input_option="RAW")
    # Write header row 2 (sub-column names)
    worksheet.update("A2", [header2], value_input_option="RAW")
    # Write data rows starting from row 3
    if data_rows:
        worksheet.update("A3", data_rows, value_input_option="USER_ENTERED")

    tz = pytz.timezone("Asia/Dhaka")
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"✅ '{worksheet_name}' updated at {timestamp}")

# ========= MAIN ==========
if __name__ == "__main__":
    login()

    for cid, cname in COMPANIES.items():
        log.info(f"\n{'='*55}")
        log.info(f"🏭 Processing: {cname} (company_id={cid})")

        if not switch_company(cid):
            log.error(f"❌ Skipping {cname} — company switch failed")
            continue

        result = fetch_ageing_data(cid, cname)

        if result:
            header1, header2, data_rows = transform_to_wide(result, cname)

            if data_rows:
                # Save locally to Excel (timestamp with seconds avoids file-lock conflicts)
                ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
                output_file = f"{cname.lower().replace(' ', '_')}_products_{ts}.xlsx"
                col_names = header1[:]
                col_names[0] = "Item Category"
                all_rows = [header2] + data_rows
                df_out = pd.DataFrame(all_rows, columns=col_names)
                df_out.to_excel(output_file, index=False)
                log.info(f"[SAVED] {output_file}  ({len(data_rows)} rows)")

                worksheet_name = WORKSHEET_MAP[cid]
                try:
                    paste_to_sheet(header1, header2, data_rows, worksheet_name, cname)
                except Exception as e:
                    log.error(f"❌ Sheets upload failed for {cname}: {e}")
            else:
                log.error(f"[ERROR] No data rows for {cname}")
        else:
            log.error(f"[ERROR] No data returned for {cname}")
