import requests
import pandas as pd
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
import os
import pytz
import logging
from google.oauth2 import service_account
import gspread
from gspread_dataframe import set_with_dataframe

# === Load .env ===
load_dotenv()

# ========= CONFIG ==========
ODOO_URL = os.getenv("ODOO_URL")
DB = os.getenv("ODOO_DB")
USERNAME = os.getenv("ODOO_USERNAME")
PASSWORD = os.getenv("ODOO_PASSWORD")

COMPANIES = {
    
    3: "Metal Trims",
    1: "Zipper"
}

# FROM_DATE = datetime.today().replace(day=1).strftime("%Y-%m-%d")
import calendar

today = date.today()
first_day_this_month = today.replace(day=1)
last_day_prev_month = first_day_this_month - timedelta(days=1)
TO_DATE = last_day_prev_month.strftime("%Y-%m-%d")
DOWNLOAD_DIR = os.path.join(os.getcwd(), "download")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# === Logging ===
logging.basicConfig(level=logging.INFO)
log = logging.getLogger()

# === Session ===
session = requests.Session()
USER_ID = None

# ========= LOGIN ==========
def login():
    global USER_ID
    payload = {
        "jsonrpc": "2.0",
        "params": {"db": DB, "login": USERNAME, "password": PASSWORD}
    }
    r = session.post(f"{ODOO_URL}/web/session/authenticate", json=payload)
    r.raise_for_status()
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
            "kwargs": {"context": {"allowed_company_ids": [company_id], "company_id": company_id}},
        },
    }
    r = session.post(f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    r.raise_for_status()
    if "error" in r.json():
        log.error(f"❌ Failed to switch company {company_id}: {r.json()['error']}")
        return False
    log.info(f"🔄 Switched to company {company_id}")
    return True

# ========= CREATE FORECAST WIZARD ==========
def create_ageing_wizard(company_id):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.ageing",
            "method": "create",
            "args": [{
                "report_type": "ageing",
                "report_for": "rm",
                "all_iteam_list": [],
                "from_date": False,
                "to_date": TO_DATE
            }],
            "kwargs": {"context": {"allowed_company_ids": [company_id], "company_id": company_id}},
        },
    }
    r = session.post(f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    r.raise_for_status()

    data = r.json()
    if "error" in data:
        log.error(f"❌ Ageing wizard creation failed: {data['error']}")
        raise Exception(data["error"])

    result = data.get("result")
    if isinstance(result, list) and len(result) > 0 and "id" in result[0]:
        wiz_id = result[0]["id"]
    else:
        raise Exception(f"Unexpected result format: {result}")

    log.info(f"🪄 Created ageing wizard {wiz_id} for company {company_id}")
    return wiz_id



# ========= COMPUTE FORECAST ==========
def compute_ageing(company_id, wizard_id):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.ageing",
            "method": "action_print_ageing_report",
            "args": [[wizard_id]],
            "kwargs": {
                "context": {
                    "lang": "en_US",
                    "tz": "Asia/Dhaka",
                    "uid": USER_ID,
                    "allowed_company_ids": [company_id],
                    "company_id": company_id,
                }
            },
        },
    }
    r = session.post(f"{ODOO_URL}/web/dataset/call_button", json=payload)
    r.raise_for_status()
    log.info(f"⚡ Ageing report computed for wizard {wizard_id} (company {company_id})")
    return r.json()


# ========= FETCH OPENING/CLOSING WITH LABELS ==========
def fetch_ageing(company_id, cname):
    context = {"allowed_company_ids": [company_id], "company_id": company_id}

    FIELD_LABELS = {
        "slot_5": "181-365",
        "slot_6": "365+",
        "lot_id_name": "Invoice",
        "lot_id_unusable": "Unusable",
    }

    # Detailed specification with display_name and unusable field for lot_id
    specification = {
        "slot_5": {},
        "slot_6": {},
        "lot_id": {"fields": {"display_name": {}, "unusable": {}}},  # Invoice with unusable field
    }

    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.ageing",
            "method": "web_search_read",
            "args": [],
            "kwargs": {
                "specification": specification,
                "offset": 0,
                "limit": 5000,
                "context": context,
                "count_limit": 10000,
                "domain": [["product_id.categ_id.complete_name", "ilike", "All / RM"]],
            },
        },
    }

    r = session.post(f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    r.raise_for_status()

    records = r.json()["result"]["records"]

    # Flatten nested lot_id fields to extract display_name and unusable separately
    def flatten(record):
        flat = {}
        for k, v in record.items():
            if k == "lot_id" and isinstance(v, dict):
                # Extract display_name and unusable as separate columns
                flat["lot_id_name"] = v.get("display_name", "")
                flat["lot_id_unusable"] = v.get("unusable", False)
            elif isinstance(v, dict) and "display_name" in v:
                flat[k] = v["display_name"]
            else:
                flat[k] = v
        return flat

    flat_records = [flatten(rec) for rec in records]

    # Convert to DataFrame and rename columns
    df = pd.DataFrame(flat_records)
    df.rename(columns=FIELD_LABELS, inplace=True)
    if "id" in df.columns:
        df.drop(columns=["id"], inplace=True)

    log.info(f"📊 {cname}: {len(df)} rows fetched (ageing report)")
    return df






# ========= PASTE TO GOOGLE SHEETS ==========
def paste_to_google_sheet(df, sheet_key, worksheet_name):
    if df.empty:
        log.warning("DataFrame empty. Skipping Google Sheet update.")
        return

    # Check if service account file exists
    if not os.path.exists("service_account.json"):
        log.warning("⚠️ service_account.json not found. Skipping Google Sheet update (data saved locally).")
        return

    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = service_account.Credentials.from_service_account_file("service_account.json", scopes=scope)
        client = gspread.authorize(creds)

        log.info(f"📝 Opening Google Sheet: {sheet_key}")
        sheet = client.open_by_key(sheet_key)

        log.info(f"📝 Accessing worksheet: {worksheet_name}")
        worksheet = sheet.worksheet(worksheet_name)

        # Step 1: Clear all existing data first
        log.info(f"🗑️ Clearing existing data in {worksheet_name}...")
        worksheet.clear()

        # Step 2: Paste new data
        log.info(f"📋 Pasting {len(df)} rows and {len(df.columns)} columns to {worksheet_name}...")
        set_with_dataframe(worksheet, df, include_index=False, include_column_header=True, resize=True)

        tz = pytz.timezone("Asia/Dhaka")
        timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

        log.info(f"✅ Successfully pasted data to {worksheet_name}")
        log.info(f"📊 Data shape: {df.shape[0]} rows × {df.shape[1]} columns")
        log.info(f"🕐 Timestamp: {timestamp}")

    except Exception as e:
        log.error(f"❌ Failed to paste data to Google Sheet '{worksheet_name}': {e}")
        raise


# ========= MAIN SYNC ==========
if __name__ == "__main__":
    login()
    for cid, cname in COMPANIES.items():
        if switch_company(cid):
            # Skip wizard creation - directly fetch existing ageing data
            df = fetch_ageing(cid, cname)

            if not df.empty:
                # Save locally
                local_file = os.path.join(DOWNLOAD_DIR, f"{cname.lower().replace(' ', '')}_ageing_{TO_DATE}.xlsx")
                df.to_excel(local_file, index=False)
                log.info(f"📂 Saved locally: {local_file}")

                # Google Sheet paste
                sheet_key = "1j37Y6g3pnMWtwe2fjTe1JTT32aRLS0Z1YPjl3v657Cc"
                worksheet_name = "unusable_zip" if cid == 1 else "unusable_MT"
                paste_to_google_sheet(df, sheet_key=sheet_key, worksheet_name=worksheet_name)
            else:
                log.warning(f"⚠️ No data fetched for {cname}")
