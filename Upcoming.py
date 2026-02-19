import requests
import time
import logging
import sys
import os
from datetime import datetime
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
DB       = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

# company_id as string (matches response field)
COMPANIES = {
    "1": "Zipper",
    "3": "Metal Trims",
}

SHEET_KEY = "1j37Y6g3pnMWtwe2fjTe1JTT32aRLS0Z1YPjl3v657Cc"

WORKSHEET_MAP = {
    "1": "Zipper_Upcoming",
    "3": "Mt_Upcoming",
}

session = requests.Session()
USER_ID = None

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
    r = retry_request(session.post, f"{ODOO_URL}/web/session/authenticate", json=payload)
    result = r.json().get("result")
    if result and "uid" in result:
        USER_ID = result["uid"]
        log.info(f"✅ Logged in (uid={USER_ID})")
        return result
    raise Exception("❌ Login failed")

# ========= SWITCH COMPANY ==========
def switch_company(company_id_int):
    if USER_ID is None:
        raise Exception("User not logged in yet")
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "res.users",
            "method": "write",
            "args": [[USER_ID], {"company_id": company_id_int}],
            "kwargs": {
                "context": {
                    "allowed_company_ids": [company_id_int],
                    "company_id": company_id_int,
                }
            },
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    if "error" in r.json():
        log.error(f"❌ Failed to switch company {company_id_int}: {r.json()['error']}")
        return False
    log.info(f"🔄 Session switched to company {company_id_int}")
    return True

# ========= FETCH RAW UPCOMING DATA ==========
def fetch_upcoming_data():
    """Fetches all rm.ageing.raw.data rows (all companies, upcoming + 180_plus buckets)."""
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "rm.ageing.raw.data",
            "method": "search_read",
            "args": [],
            "kwargs": {
                "order": "period asc, item_category asc",
                "domain": [],
                "fields": [
                    "company_id", "item_category", "classification",
                    "product_id", "lot_id", "bucket", "period",
                    "closing_value", "current_value", "utilization",
                ],
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": USER_ID,
                    "allowed_company_ids": [int(k) for k in COMPANIES],
                },
            },
        },
    }
    r = retry_request(
        session.post,
        f"{ODOO_URL}/web/dataset/call_kw/rm.ageing.raw.data/search_read",
        json=payload,
    )
    result = r.json().get("result", [])
    log.info(f"📊 Fetched {len(result)} raw rows (all companies)")
    return result

# ========= TRANSFORM TO WIDE FORMAT ==========
def transform_to_wide(raw_rows, company_id_str, cname):
    """
    Builds wide format matching original report:
      Header row 1: Item Category | 180+ | 180+ | Feb-2026 | Feb-2026 | Mar-2026 | Mar-2026 | ...
      Header row 2: (empty)       | Closing | Utilization | Closing | Status | Closing | Status | ...
      Data rows   : one per item_category

    180+ columns  → sum(closing_value), sum(utilization)
    Period columns → sum(closing_value) = Closing, sum(current_value) = Status
    """
    if not raw_rows:
        log.warning(f"No raw data to process for {cname}")
        return [], [], []

    df = pd.DataFrame(raw_rows)
    df = df[df["company_id"].astype(str) == company_id_str].copy()

    if df.empty:
        log.warning(f"No data for {cname}")
        return [], [], []

    # ---- Upcoming buckets only ----
    df_up = df[df["bucket"].str.startswith("upcoming")].copy()

    if df_up.empty:
        log.warning(f"No upcoming rows for {cname}")
        return [], [], []

    # Chronologically sorted upcoming period labels
    df_up["period_dt"] = pd.to_datetime(df_up["period"], format="%b-%Y")
    periods_sorted = (
        df_up[["period_dt", "period"]]
        .drop_duplicates()
        .sort_values("period_dt")
    )
    period_labels = periods_sorted["period"].tolist()

    # Unique item categories from upcoming data
    categories = sorted(df_up["item_category"].dropna().unique())

    # Pivot table
    pup_status = df_up.groupby(["item_category", "period"])["current_value"].sum()

    # Build 2-row headers (upcoming months only)
    header1 = ["Item Category"]
    header2 = [""]
    for p in period_labels:
        header1 += [p]
        header2 += ["Current"]

    # Build data rows
    data_rows = []
    for cat in categories:
        row = [cat]
        for p in period_labels:
            try:
                status = round(float(pup_status.loc[(cat, p)]), 4)
            except KeyError:
                status = 0.0
            row.append(status)
        data_rows.append(row)

    log.info(
        f"📐 {cname}: {len(categories)} categories × "
        f"(180+ + {len(period_labels)} upcoming periods) → wide table ready"
    )
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
    sheet  = client.open_by_key(SHEET_KEY)
    worksheet = sheet.worksheet(worksheet_name)

    worksheet.batch_clear(["A:ZZ"])
    worksheet.update("A1", [header1], value_input_option="RAW")
    worksheet.update("A2", [header2], value_input_option="RAW")
    worksheet.update("A3", data_rows,  value_input_option="USER_ENTERED")

    tz = pytz.timezone("Asia/Dhaka")
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"✅ '{worksheet_name}' updated at {timestamp}")

# ========= MAIN ==========
if __name__ == "__main__":
    login()

    # Fetch all upcoming data in a single API call (covers both companies)
    raw_rows = fetch_upcoming_data()

    if not raw_rows:
        log.error("[ERROR] No data returned from API")
        raise SystemExit(1)

    for cid_str, cname in COMPANIES.items():
        log.info(f"\n{'='*55}")
        log.info(f"🏭 Processing: {cname} (company_id={cid_str})")

        if not switch_company(int(cid_str)):
            log.error(f"❌ Skipping {cname} — company switch failed")
            continue

        header1, header2, data_rows = transform_to_wide(raw_rows, cid_str, cname)

        if data_rows:
            # Save locally to Excel (header1 as columns, header2 + data as rows)
            ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            output_file = f"{cname.lower().replace(' ', '_')}_upcoming_{ts}.xlsx"
            col_names = header1[:]
            col_names[0] = "Item Category"
            all_rows = [header2] + data_rows
            df_out = pd.DataFrame(all_rows, columns=col_names)
            df_out.to_excel(output_file, index=False)
            log.info(f"[SAVED] {output_file}  ({len(data_rows)} rows)")

            # Push to Google Sheets
            worksheet_name = WORKSHEET_MAP[cid_str]
            try:
                paste_to_sheet(header1, header2, data_rows, worksheet_name, cname)
            except Exception as e:
                log.error(f"❌ Sheets upload failed for {cname}: {e}")
        else:
            log.error(f"[ERROR] No upcoming data rows for {cname}")
