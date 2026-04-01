#!/usr/bin/env python3
"""
update_steven_scorecard.py
Pulls nurture dispositions + appointment show/no-show data from Podio
and pushes data.json to the uig-steven-scorecard GitHub repo.
"""

import os
import json, urllib.request, urllib.parse, base64, warnings
from datetime import datetime
from collections import defaultdict

warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────
PODIO_CLIENT_ID  = "atlas"
PODIO_SECRET     = os.environ["PODIO_SECRET"]
PODIO_APP_ID     = "25179555"
PODIO_APP_TOKEN  = os.environ["PODIO_APP_TOKEN"]
NURTURE_APP_ID   = "30453470"
APPT_APP_ID      = "29988133"

GITHUB_TOKEN     = os.environ["PUSH_TOKEN"]
GITHUB_REPO      = "nestor-lang/uig-steven-scorecard"
GITHUB_FILE      = "data.json"

today_str = datetime.now().strftime("%Y-%m-%d")

# ── Helpers ───────────────────────────────────────────────────────────────────
def post(url, data, headers=None, form=False):
    body = urllib.parse.urlencode(data).encode() if form else json.dumps(data).encode()
    ct = "application/x-www-form-urlencoded" if form else "application/json"
    req = urllib.request.Request(url, data=body,
        headers={"Content-Type": ct, **(headers or {})}, method="POST")
    return json.loads(urllib.request.urlopen(req, timeout=30).read())

def get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    return json.loads(urllib.request.urlopen(req, timeout=30).read())

def get_github_sha(path):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    try:
        req = urllib.request.Request(url, headers={"Authorization": f"token {GITHUB_TOKEN}"})
        r = json.loads(urllib.request.urlopen(req, timeout=10).read())
        return r.get("sha")
    except:
        return None

def put_github(path, content_str, sha=None):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    payload = {
        "message": f"chore: update {path} [{today_str}]",
        "content": base64.b64encode(content_str.encode()).decode(),
        "branch": "main"
    }
    if sha:
        payload["sha"] = sha
    req = urllib.request.Request(url, data=json.dumps(payload).encode(),
        headers={"Authorization": f"token {GITHUB_TOKEN}", "Content-Type": "application/json"},
        method="PUT")
    return json.loads(urllib.request.urlopen(req, timeout=20).read())

def month_key(date_str):
    """Convert '2026-03-15' or datetime string to 'Mar 2026'"""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return dt.strftime("%b %Y")
    except:
        return None

def post_with_retry(url, data, headers, retries=3, timeout=45):
    body = json.dumps(data).encode()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=body,
                headers={"Content-Type": "application/json", **(headers or {})}, method="POST")
            return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
        except Exception as e:
            if attempt < retries - 1:
                import time; time.sleep(3)
            else:
                raise

def fetch_all(app_id, headers, extra_body=None):
    """Fetch all items from a Podio app via pagination with retry."""
    all_items = []
    offset = 0
    body = {"limit": 200, "sort_by": "created_on", "sort_desc": False}
    if extra_body:
        body.update(extra_body)
    while True:
        body["offset"] = offset
        result = post_with_retry(f"https://api.podio.com/item/app/{app_id}/filter/", body, headers)
        items = result.get("items", [])
        all_items.extend(items)
        print(f"  Fetched {len(all_items)} / {result.get('total', '?')}...")
        if len(items) < 200:
            break
        offset += 200
    return all_items

# ── Auth ──────────────────────────────────────────────────────────────────────
print("Authenticating with Podio...")
auth = post("https://podio.com/oauth/token", {
    "grant_type": "app", "app_id": PODIO_APP_ID, "app_token": PODIO_APP_TOKEN,
    "client_id": PODIO_CLIENT_ID, "client_secret": PODIO_SECRET
}, form=True)
token = auth["access_token"]
podio_headers = {"Authorization": f"Bearer {token}"}
print("  Auth OK")

# ── Load existing data.json from GitHub (to preserve completed months) ────────
print("Loading existing data from GitHub...")
existing_data = {}
try:
    req = urllib.request.Request(
        f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}",
        headers={"Authorization": f"token {GITHUB_TOKEN}"}
    )
    r = json.loads(urllib.request.urlopen(req, timeout=10).read())
    existing_data = json.loads(base64.b64decode(r["content"]).decode())
    print(f"  Loaded {len(existing_data)} existing months")
except Exception as e:
    print(f"  No existing data or error: {e}")

# ── 1. Nurture dispositions (current month only) ──────────────────────────────
DISP_KEYS = [
    "Contact - Wrong Number",
    "Contact - Dead",
    "Contact - Passive",
    "Contact - Live Transfer",
    "Contact - Appointment Booked",
    "No Contact - Passive",
    "Non-Disposed Yet",
]

disp_by_month = defaultdict(lambda: defaultdict(int))

now = datetime.now()
current_mk = now.strftime("%b %Y")

# Only pull the current month — past months are cached
months_to_pull = [(now.year, now.month)]

print("Pulling nurture items (current month only)...")
for yr, mo in months_to_pull:
    from_date = f"{yr}-{mo:02d}-01 00:00:00"
    # last day of month
    if mo == 12:
        to_date = f"{yr}-12-31 23:59:59"
    else:
        import calendar
        last_day = calendar.monthrange(yr, mo)[1]
        to_date = f"{yr}-{mo:02d}-{last_day} 23:59:59"

    mk = datetime(yr, mo, 1).strftime("%b %Y")
    all_items = []
    offset = 0
    while True:
        body = {
            "limit": 200, "offset": offset,
            "sort_by": "created_on", "sort_desc": False,
            "filters": {"lead-created": {"from": from_date, "to": to_date}}
        }
        result = post_with_retry(f"https://api.podio.com/item/app/{NURTURE_APP_ID}/filter/",
            body, podio_headers)
        items = result.get("items", [])
        all_items.extend(items)
        if len(items) < 200:
            break
        offset += 200

    print(f"  {mk}: {len(all_items)} items")
    for item in all_items:
        fields = {f["external_id"]: f.get("values") for f in item.get("fields", [])}
        disp_vals = fields.get("dispositions") or []
        if disp_vals:
            disp_text = disp_vals[0].get("value", {})
            if isinstance(disp_text, dict):
                disp_text = disp_text.get("text", "Non-Disposed Yet")
            else:
                disp_text = str(disp_text) if disp_text else "Non-Disposed Yet"
        else:
            disp_text = "Non-Disposed Yet"
        disp_by_month[mk][disp_text] += 1

print(f"  Dispositions aggregated across {len(disp_by_month)} months")

# ── 2a. Total LT + Appt Booked by disposition-date (payout count) ────────────
print("Pulling nurture payout count (disposed this month as LT or Appt Booked)...")

import calendar
last_day = calendar.monthrange(now.year, now.month)[1]
disp_from = f"{now.year}-{now.month:02d}-01 00:00:00"
disp_to   = f"{now.year}-{now.month:02d}-{last_day} 23:59:59"

PAYOUT_DISPS = {"Contact - Live Transfer", "Contact - Appointment Booked"}
payout_count = 0
payout_lt = 0
payout_appt = 0

offset = 0
while True:
    body = {
        "limit": 200, "offset": offset,
        "sort_by": "created_on", "sort_desc": False,
        "filters": {"disposition-date": {"from": disp_from, "to": disp_to}}
    }
    result = post_with_retry(f"https://api.podio.com/item/app/{NURTURE_APP_ID}/filter/",
        body, podio_headers)
    items = result.get("items", [])
    for item in items:
        fields = {f["external_id"]: f.get("values") for f in item.get("fields", [])}
        disp_vals = fields.get("dispositions") or []
        if disp_vals:
            disp_text = disp_vals[0].get("value", {})
            if isinstance(disp_text, dict):
                disp_text = disp_text.get("text", "")
            else:
                disp_text = str(disp_text) if disp_text else ""
            if disp_text == "Contact - Live Transfer":
                payout_lt += 1
                payout_count += 1
            elif disp_text == "Contact - Appointment Booked":
                payout_appt += 1
                payout_count += 1
    if len(items) < 200:
        break
    offset += 200

print(f"  {current_mk}: {payout_count} payout bookings (LT: {payout_lt}, Appt: {payout_appt})")

# ── 2b. Show rate — appointments booked this month (Initial Discovery) ────────
print("Pulling appointments (Initial Discovery, current month)...")

appt_from = f"{now.year}-{now.month:02d}-01 00:00:00"
appt_to   = f"{now.year}-{now.month:02d}-{last_day} 23:59:59"

showed_by_month = defaultdict(int)
appt_booked_count = 0

offset = 0
while True:
    body = {
        "limit": 200, "offset": offset,
        "sort_by": "created_on", "sort_desc": False,
        "filters": {"invitee-created-at": {"from": appt_from, "to": appt_to}}
    }
    result = post_with_retry(f"https://api.podio.com/item/app/{APPT_APP_ID}/filter/",
        body, podio_headers)
    items = result.get("items", [])
    for item in items:
        fields = {f["external_id"]: f.get("values") for f in item.get("fields", [])}
        event_name_vals = fields.get("event-name") or []
        event_name = event_name_vals[0].get("value", "") if event_name_vals else ""
        if "initial discovery" not in event_name.lower():
            continue
        appt_booked_count += 1
        status2 = fields.get("status-2") or []
        if status2:
            s = status2[0].get("value", {})
            if isinstance(s, dict) and s.get("text", "").lower() == "show":
                showed_by_month[current_mk] += 1
    if len(items) < 200:
        break
    offset += 200

print(f"  {current_mk}: {appt_booked_count} appts booked, {showed_by_month.get(current_mk, 0)} showed")

# ── 3. Build data.json ────────────────────────────────────────────────────────
print("Building data.json...")

def month_sort_key(m):
    try:
        return datetime.strptime(m, "%b %Y")
    except:
        return datetime.min

# Start with existing cached data, then overwrite only current month
data = dict(existing_data)

# Update current month with fresh data
disps = dict(disp_by_month.get(current_mk, {}))
for k in DISP_KEYS:
    if k not in disps:
        disps[k] = 0
data[current_mk] = {
    "dispositions": disps,
    "totalBooked": appt_booked_count,   # Initial Discovery appts created this month (show rate base)
    "totalLT": payout_lt,               # LT disposed this month (payout)
    "totalApptBooked": payout_appt,     # Appt Booked disposed this month (payout)
    "totalShowed": showed_by_month.get(current_mk, 0),
}

# Re-sort all months
all_months = sorted(data.keys(), key=month_sort_key)
data = {mk: data[mk] for mk in all_months}

content_str = json.dumps(data, indent=2)

# ── 4. Push to GitHub ─────────────────────────────────────────────────────────
print("Pushing to GitHub...")
sha = get_github_sha(GITHUB_FILE)
result = put_github(GITHUB_FILE, content_str, sha)
commit = result.get("commit", {}).get("sha", "")[:8]

latest_mk = all_months[-1] if all_months else "N/A"
latest = data.get(latest_mk, {})
lt  = latest.get("dispositions", {}).get("Contact - Live Transfer", 0)
apb = latest.get("dispositions", {}).get("Contact - Appointment Booked", 0)

print(f"✅ Scorecard updated: https://nestor-lang.github.io/uig-steven-scorecard/")
print(f"   Commit: {commit}")
print(f"   Months: {len(all_months)} | Latest: {latest_mk}")
print(f"   LT+Appt Booked (payout): {latest.get('totalBooked',0)} | Showed: {latest.get('totalShowed',0)}")
