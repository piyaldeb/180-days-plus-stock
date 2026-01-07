import requests
import pandas as pd
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
import os
import pytz
import logging
import sys
from google.oauth2 import service_account
import gspread
from gspread_dataframe import set_with_dataframe
import time
from requests.exceptions import RequestException

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

# === Logging ===
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger()

# === Session ===
session = requests.Session()
USER_ID = None

today = date.today()
first_day_this_month = today.replace(day=1)
last_day_prev_month = first_day_this_month - timedelta(days=1)
TO_DATE = last_day_prev_month.strftime("%Y-%m-%d")
FROM_DATE = False

DOWNLOAD_DIR = os.path.join(os.getcwd(), "download")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ========= RETRY WRAPPER ==========
def retry_request(method, url, max_retries=3, backoff=3, **kwargs):
    for attempt in range(1, max_retries + 1):
        try:
            r = method(url, **kwargs)
            r.raise_for_status()
            return r
        except RequestException as e:
            log.warning(f"⚠️ Attempt {attempt} failed: {e}")
            if attempt < max_retries:
                log.info(f"⏳ Retrying in {backoff} seconds...")
                time.sleep(backoff)
            else:
                log.error("❌ All retry attempts failed.")
                raise

# ========= LOGIN ==========
def login():
    global USER_ID
    payload = {"jsonrpc": "2.0", "params": {"db": DB, "login": USERNAME, "password": PASSWORD}}
    r = retry_request(session.post, f"{ODOO_URL}/web/session/authenticate", json=payload)
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
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    if "error" in r.json():
        log.error(f"❌ Failed to switch company {company_id}: {r.json()['error']}")
        return False
    log.info(f"🔄 Switched to company {company_id}")
    return True

# ========= CREATE AGEING WIZARD ==========
def create_ageing_wizard(company_id, from_date, to_date):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.forecast.report",
            "method": "web_save",
            "args": [[], {
                "report_type": "ageing",
                "report_for": "rm",
                "all_iteam_list": [],
                "from_date": from_date,
                "to_date": to_date
            }],
            "kwargs": {
                "context": {"lang": "en_US", "tz": "Asia/Dhaka", "uid": USER_ID,
                            "allowed_company_ids": [company_id], "company_id": company_id},
                "specification": {
                    "report_type": {},
                    "report_for": {},
                    "all_iteam_list": {"fields": {"display_name": {}}},
                    "from_date": {},
                    "to_date": {},
                },
            },
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw/stock.forecast.report/web_save", json=payload)
    result = r.json().get("result", [])
    if isinstance(result, list) and result:
        wiz_id = result[0]["id"]
        log.info(f"🪄 Ageing wizard {wiz_id} created for company {company_id}")
        return wiz_id
    else:
        raise Exception(f"❌ Failed to create ageing wizard: {r.text}")

# ========= COMPUTE AGEING ==========
def compute_ageing(company_id, wizard_id):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.forecast.report",
            "method": "print_date_wise_stock_register",
            "args": [[wizard_id]],
            "kwargs": {"context": {"lang": "en_US", "tz": "Asia/Dhaka",
                                   "uid": USER_ID,
                                   "allowed_company_ids": [company_id],
                                   "company_id": company_id}},
        },
    }
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_button", json=payload)
    result = r.json()
    if "error" in result:
        log.error(f"❌ Error computing ageing for {company_id}: {result['error']}")
    else:
        log.info(f"⚡ Ageing computed for wizard {wizard_id} (company {company_id})")
    return result

# ========= FETCH AGEING REPORT (3 COLUMNS ONLY) ==========
def fetch_ageing(company_id, cname, wizard_id):
    context = {"allowed_company_ids": [company_id], "company_id": company_id,
               "active_model": "stock.forecast.report", "active_id": wizard_id, "active_ids": [wizard_id]}
    
    specification = {
        "slot_5": {},
        "slot_6": {},
        "lot_id": {"fields": {"display_name": {}, "unusable": {}}},
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
                "domain": [
                    ["product_id.categ_id.complete_name", "ilike", "All / RM"],
                    "|",
                    ["slot_5", ">", 0],
                    ["slot_6", ">", 0]
                ],
            },
        },
    }
    
    log.info(f"🔍 Fetching ageing data for {cname} (company_id={company_id})...")
    r = retry_request(session.post, f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    
    result = r.json().get("result", {})
    records = result.get("records", [])
    
    log.info(f"📊 {cname}: Fetched {len(records)} records with slot_5 or slot_6 > 0")
    
    def flatten(record):
        flat = {}
        for k, v in record.items():
            if k == "lot_id" and isinstance(v, dict):
                flat["Invoice"] = v.get("display_name", "")
                flat["Unusable"] = v.get("unusable", False)
            elif k == "slot_5":
                flat["181-365"] = v
            elif k == "slot_6":
                flat["365+"] = v
        return flat
    
    flattened = [flatten(rec) for rec in records]
    df = pd.DataFrame(flattened)
    
    if "id" in df.columns:
        df.drop(columns=["id"], inplace=True)
    
    log.info(f"📊 {cname}: {len(df)} rows in final dataframe")
    return df

# ========= PASTE TO GOOGLE SHEETS ==========
def paste_to_google_sheet(df, sheet_key, worksheet_name):
    if not os.path.exists("service_account.json"):
        log.warning("⚠️ service_account.json not found. Skipping Google Sheet update.")
        return

    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = service_account.Credentials.from_service_account_file("service_account.json", scopes=scope)
        client = gspread.authorize(creds)

        log.info(f"📝 Opening Google Sheet: {sheet_key}")
        sheet = client.open_by_key(sheet_key)
        worksheet = sheet.worksheet(worksheet_name)

        log.info(f"🗑️ Clearing existing data in {worksheet_name}...")
        worksheet.clear()

        if df.empty:
            log.warning(f"⚠️ No data to paste for {worksheet_name}. Sheet has been cleared.")
        else:
            log.info(f"📋 Pasting {len(df)} rows to {worksheet_name}...")
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
    userinfo = login()
    log.info(f"User info (allowed companies): {userinfo.get('user_companies', {})}")

    for cid, cname in COMPANIES.items():
        log.info(f"\n🚀 Processing company: {cname} (ID={cid})")
        
        if switch_company(cid):
            # Create wizard and compute ageing report
            wiz_id = create_ageing_wizard(cid, FROM_DATE, TO_DATE)
            compute_ageing(cid, wiz_id)
            
            # Fetch only 3 columns of data
            df = fetch_ageing(cid, cname, wiz_id)

            # Save locally
            if not df.empty:
                local_file = os.path.join(DOWNLOAD_DIR, f"{cname.lower().replace(' ', '')}_ageing_{TO_DATE}.xlsx")
                df.to_excel(local_file, index=False)
                log.info(f"📂 Saved locally: {local_file}")
            else:
                log.warning(f"⚠️ No data available for {cname}")

            # Update Google Sheet
            sheet_key = "1j37Y6g3pnMWtwe2fjTe1JTT32aRLS0Z1YPjl3v657Cc"
            worksheet_name = "unusable_zip" if cid == 1 else "unusable_MT"
            paste_to_google_sheet(df, sheet_key=sheet_key, worksheet_name=worksheet_name)
