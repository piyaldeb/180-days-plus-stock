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
        "parent_category": "Product",
        "product_category": "Category",
        "product_id": "Item",
        "lot_id": "Invoice",
        "receive_date": "Receive Date",
        "shipment_mode": "Shipment Mode",
        "slot_1": "0-30",
        "slot_2": "31-60",
        "slot_3": "61-90",
        "slot_4": "91-180",
        "slot_5": "181-365",
        "slot_6": "365+",
        "duration": "Duration",
        "cloing_qty": "Quantity",
        "cloing_value": "Value",
        "landed_cost": "Landed Cost",
        "lot_price": "Price",
        "pur_price": "Pur Price",
        "rejected": "Rejected",
        "company_id": "Company",
    }

    # Detailed specification with display_name for relevant fields
    specification = {
        "parent_category": {"fields": {"display_name": {}}},   # Product
        "product_category": {"fields": {"display_name": {}}},  # Category
        "product_id": {"fields": {"display_name": {}}},        # Item
        "lot_id": {"fields": {"display_name": {}}},            # Invoice
        "receive_date": {},                                    # Receive Date
        "shipment_mode": {},                                   # Shipment Mode
        "slot_1": {}, "slot_2": {}, "slot_3": {},
        "slot_4": {}, "slot_5": {}, "slot_6": {},
        "duration": {}, "cloing_qty": {}, "cloing_value": {},
        "landed_cost": {}, "lot_price": {}, "pur_price": {},
        "rejected": {}, "company_id": {"fields": {"display_name": {}}},
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

    # Flatten nested display_name fields
    def flatten(record):
        flat = {}
        for k, v in record.items():
            if isinstance(v, dict) and "display_name" in v:
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

    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file("service_account.json", scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_key)
    worksheet = sheet.worksheet(worksheet_name)

    # Clear only columns A → T (20 columns)
    worksheet.batch_clear(["A:T"])

    # Paste data
    set_with_dataframe(worksheet, df)

    tz = pytz.timezone("Asia/Dhaka")
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    # # Put timestamp in column after last df column (safe up to Z)
    # last_col_idx = min(26, df.shape[1])  # max 26 (A-Z)
    # last_col_letter = chr(65 + last_col_idx - 1)
    # worksheet.update(f"{last_col_letter}2", [[timestamp]])

    log.info(f"✅ Data pasted to {worksheet_name} & timestamp updated: {timestamp}")


# ========= MAIN SYNC ==========
if __name__ == "__main__":
    login()
    for cid, cname in COMPANIES.items():
        if switch_company(cid):
            df = fetch_ageing(cid, cname)

            if not df.empty:
                # Save locally
                local_file = os.path.join(DOWNLOAD_DIR, f"{cname.lower().replace(' ', '')}_ageing_{TO_DATE}.xlsx")
                df.to_excel(local_file, index=False)
                log.info(f"📂 Saved locally: {local_file}")

                # Google Sheet paste
                sheet_key = "1j37Y6g3pnMWtwe2fjTe1JTT32aRLS0Z1YPjl3v657Cc"
                worksheet_name = "Closing Stock" if cid == 1 else "Closing Stock - MT"
                paste_to_google_sheet(df, sheet_key=sheet_key, worksheet_name=worksheet_name)
