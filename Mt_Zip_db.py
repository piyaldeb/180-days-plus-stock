import requests
import pandas as pd
from datetime import date, datetime
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
    1: "Zipper",
    3: "Metal Trims",
}

FROM_DATE = datetime.today().replace(day=1).strftime("%Y-%m-%d")
TO_DATE = date.today().strftime("%Y-%m-%d")
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
def create_forecast_wizard(company_id):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.forecast.report",
            "method": "create",
            "args": [{"from_date": FROM_DATE, "to_date": TO_DATE}],
            "kwargs": {"context": {"allowed_company_ids": [company_id], "company_id": company_id}},
        },
    }
    r = session.post(f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    r.raise_for_status()
    wiz_id = r.json()["result"]
    log.info(f"🪄 Created wizard {wiz_id} for company {company_id}")
    return wiz_id

# ========= COMPUTE FORECAST ==========
def compute_forecast(company_id, wizard_id):
    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.forecast.report",
            "method": "print_date_wise_stock_register",
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
    log.info(f"⚡ Forecast computed for wizard {wizard_id} (company {company_id})")
    return r.json()

# ========= FETCH OPENING/CLOSING WITH LABELS ==========
def fetch_opening_closing(company_id, cname):
    context = {"allowed_company_ids": [company_id], "company_id": company_id}

    # Updated specification with new fields
    specification = {
        "product_category": {"fields": {"display_name": {}}},   # Category
        "classification_id": {"fields": {"display_name": {}}},  # Classification
        "cloing_qty": {},                                       # Closing Quantity
        "cloing_value": {},                                     # Closing Value
        "lot_id": {"fields": {"display_name": {}}},             # Invoice
        "issue_qty": {},                                        # Issue Quantity
        "issue_value": {},                                      # Issue Value
        "product_id": {"fields": {"display_name": {}}},         # Item
        "pr_code": {},                                          # Item Code
        "landed_cost": {},                                      # Landed Cost
        "opening_qty": {},                                      # Opening Quantity
        "opening_value": {},                                    # Opening Value
        "po_type": {},                                          # Po Type
        "lot_price": {},                                        # Price
        "parent_category": {"fields": {"display_name": {}}},    # Product
        "pur_price": {},                                        # Pur Price
        "receive_date": {},                                     # Receive Date
        "receive_qty": {},                                      # Receive Quantity
        "receive_value": {},                                    # Receive Value
        "rejected": {},                                         # Rejected
        "shipment_mode": {},                                    # Shipment Mode
        "product_uom": {"fields": {"display_name": {}}},        # Unit
        "partner_id": {"fields": {"display_name": {}}},         # Vendor
        "po_number": {},                                        # PO
        "product_type": {},                                     # Product Type
        "item_category": {},                                    # Item Type
    }

    payload = {
        "jsonrpc": "2.0",
        "method": "call",
        "params": {
            "model": "stock.opening.closing",
            "method": "web_search_read",
            "args": [],
            "kwargs": {
                "specification": specification,
                "offset": 0,
                "limit": 5000,
                "context": {
                    **context,
                    "active_model": "stock.forecast.report",
                    "active_id": 0,
                    "active_ids": [0],
                },
                "count_limit": 10000,
                "domain": [["product_id.categ_id.complete_name", "ilike", "All / RM"]],
            },
        },
    }

    r = session.post(f"{ODOO_URL}/web/dataset/call_kw", json=payload)
    r.raise_for_status()

    try:
        records = r.json()["result"]["records"]

        # Flatten nested dicts → keep only display_name
        def flatten(record):
            flat = {}
            for k, v in record.items():
                if isinstance(v, dict) and "display_name" in v:
                    flat[k] = v["display_name"]
                else:
                    flat[k] = v
            return flat

        flattened = [flatten(rec) for rec in records]

        # Convert to DataFrame
        df = pd.DataFrame(flattened)

        # Drop unwanted 'id' column if exists
        if "id" in df.columns:
            df.drop(columns=["id"], inplace=True)

        # Updated field → label mapping
        FIELD_LABELS = {
            "parent_category": "Product",
            "product_category": "Category",
            "classification_id": "Classification",
            "product_id": "Item",
            "pr_code": "Item Code",
            "lot_id": "Invoice",
            "receive_date": "Receive Date",
            "pur_price": "Pur Price",
            "landed_cost": "Landed Cost",
            "lot_price": "Price",
            "product_uom": "Unit",
            "opening_qty": "Opening Quantity",
            "opening_value": "Opening Value",
            "receive_qty": "Receive Quantity",
            "receive_value": "Receive Value",
            "issue_qty": "Issue Quantity",
            "issue_value": "Issue Value",
            "cloing_qty": "Closing Quantity",
            "cloing_value": "Closing Value",
            "po_type": "Po Type",
            "rejected": "Rejected",
            "shipment_mode": "Shipment Mode",
            "partner_id": "Vendor",
            "po_number": "PO",
            "product_type": "Product Type",
            "item_category": "Item Type",
        }

        df.rename(columns=FIELD_LABELS, inplace=True)

        log.info(f"📊 {cname}: {len(df)} rows fetched with labels")
        return df

    except Exception as e:
        log.error(f"❌ {cname}: Failed to parse report: {r.text[:200]} | Error: {e}")
        return pd.DataFrame()






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

    # Clear only columns A → Z
    worksheet.batch_clear(["A:Z"])

    # Paste data
    set_with_dataframe(worksheet, df)

    tz = pytz.timezone("Asia/Dhaka")
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    # Put timestamp in column after last df column (safe up to Z)
    last_col_idx = min(26, df.shape[1])  # max 26 (A-Z)
    last_col_letter = chr(65 + last_col_idx - 1)
    worksheet.update(f"{last_col_letter}2", [[timestamp]])

    log.info(f"✅ Data pasted to {worksheet_name} & timestamp updated: {timestamp}")


# ========= MAIN SYNC ==========
if __name__ == "__main__":
    login()
    for cid, cname in COMPANIES.items():
        if switch_company(cid):
            wiz_id = create_forecast_wizard(cid)
            compute_forecast(cid, wiz_id)
            df = fetch_opening_closing(cid, cname)
            if not df.empty:
                # Save locally
                local_file = os.path.join(DOWNLOAD_DIR, f"{cname.lower().replace(' ', '')}_opening_closing_{TO_DATE}.xlsx")
                df.to_excel(local_file, index=False)
                log.info(f"📂 Saved locally: {local_file}")

                # Sheet key for both companies
                sheet_key = "1j37Y6g3pnMWtwe2fjTe1JTT32aRLS0Z1YPjl3v657Cc"

                # Worksheet name based on company
                if cid == 1:
                    worksheet_name = "Current Stock report"
                elif cid == 3:
                    worksheet_name = "Current Stock - MT"
                else:
                    worksheet_name = cname

                # Paste to Google Sheets
                paste_to_google_sheet(df, sheet_key=sheet_key, worksheet_name=worksheet_name)
