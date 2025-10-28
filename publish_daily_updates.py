import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import hashlib
import json
import os
import base64
import requests
from bs4 import BeautifulSoup
from typing import Dict, Any, List, Optional
import re

# --- 1. SECURE CREDENTIALS AND CONFIGURATION ---
# This function securely initializes Firebase using the GitHub Secret.

def initialize_firebase_securely() -> firestore.client | None:
    """Initializes Firebase Admin SDK using a base64-encoded environment secret."""
    try:
        # Get the Base64 encoded key from the environment (e.g., GitHub Actions Secret)
        encoded_key = os.environ.get('FIREBASE_ADMIN_KEY_B64')
        if not encoded_key:
            print("FATAL ERROR: FIREBASE_ADMIN_KEY_B64 environment variable not set. Aborting.")
            return None
            
        # Decode the key content (from Base64 string back to JSON bytes)
        service_account_info = json.loads(base64.b64decode(encoded_key))

        # Use credentials.Certificate to load the JSON content directly
        cred = credentials.Certificate(service_account_info)
        
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        
        return firestore.client()
        
    except Exception as e:
        print(f"FATAL ERROR: Failed to initialize Firebase. Check GitHub Secret format. Details: {e}")
        return None

# Initialize Firestore client globally
db = initialize_firebase_securely()

# --- 2. FIRESTORE PUBLISHING LOGIC ---
APP_ID = "nsefi-policy-tracker"
COLLECTION_PATH = f'artifacts/{APP_ID}/public/data/policies'

def get_unique_document_id(title: str, date_str: str) -> str:
    """Creates a stable, URL-safe ID using a hash of the policy title and date."""
    key_string = f"{title.strip().lower()}-{date_str}"
    hash_value = hashlib.sha1(key_string.encode('utf-8')).hexdigest()
    return f"{date_str}-{hash_value[:10]}"

def transform_and_publish_policies(policies_snapshot: Dict[str, Any]) -> int:
    """
    Transforms the categorized snapshot data into flat documents and pushes them to Firestore.
    """
    if not db:
        print("ERROR: Firestore client is not available. Cannot publish data.")
        return 0

    policy_list: List[Dict[str, Any]] = []
    
    # 1. Flatten the structure
    for source_type, sources in policies_snapshot.items():
        if source_type in ['central', 'states', 'uts']:
            for source_name, items in sources.items():
                for item in items:
                    document = {
                        "title": item.get('title', 'Untitled Policy'),
                        "url": item.get('url', '#'),
                        "summary": item.get('summary', 'No summary available.'),
                        "source": item.get('source', source_name),
                        "category": item.get('category', 'Regulation'),
                        "source_type": source_type.capitalize().replace('s', ''), 
                        "publication_date": item.get('date', datetime.utcnow().strftime("%Y-%m-%d")),
                        "published_at": datetime.utcnow().isoformat()
                    }
                    policy_list.append(document)

    print(f"STATUS: Publishing {len(policy_list)} policy documents to {COLLECTION_PATH}...")

    # 2. Perform a Batch Write Operation
    batch = db.batch()
    collection_ref = db.collection(COLLECTION_PATH)
    
    for policy in policy_list:
        doc_id = get_unique_document_id(policy['title'], policy['publication_date'])
        doc_ref = collection_ref.document(doc_id)
        batch.set(doc_ref, policy)
    
    batch.commit()
    return len(policy_list)


# --- 3. CTUIL SCRAPING LOGIC ---

CTUIL_LATEST_URL = "https://ctuil.in/latestnews?p=ajax"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://ctuil.in",
    "Referer": "https://ctuil.in/latestnews",
}

def _clean_text(s: str) -> str:
    """Standardizes whitespace and strips."""
    return re.sub(r"\s+", " ", s).strip()

def _parse_date_ddmmyyyy(raw: str) -> Optional[datetime]:
    """Parses date strings like '14.10.2025'."""
    if not raw: return None
    raw = raw.strip().replace(".", "-").replace("/", "-")
    try:
        return datetime.strptime(raw, '%d-%m-%Y')
    except ValueError:
        return None

def _fetch_html(url: str, payload: dict) -> str:
    """Fetches HTML using a POST request."""
    try:
        with requests.Session() as s:
            s.headers.update(HEADERS)
            resp = s.post(url, data=payload, timeout=20)
            resp.raise_for_status()
            return resp.text
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch CTUIL page. Error: {e}")
        return ""

def _harvest_ctuil_month(year: int, month: int) -> List[Dict[str, str]]:
    """Scrapes CTUIL and filters by the requested month."""
    payload = {
        'sort_field': 'LatestNews.news_date',
        'sort_type': 'DESC',
        'page': '1',
        'search_keyword': '',
        'from_date': '',
        'to_date': '',
    }
    
    html = _fetch_html(CTUIL_LATEST_URL, payload)
    if not html: return []

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table: return []

    items: List[Dict[str, str]] = []
    
    for tr in table.find_all("tr")[1:]: # Skip header row
        tds = tr.find_all("td")
        if len(tds) < 3: continue

        date_text = _clean_text(tds[1].text)
        title_cell = tds[2]
        title_text = _clean_text(title_cell.text)
        a_tag = title_cell.find("a", href=True)
        
        dt = _parse_date_ddmmyyyy(date_text)
        
        # Filter logic: Only include items from the specified month/year
        if dt and dt.year == year and dt.month == month and a_tag:
            items.append({
                "date": dt.strftime("%Y-%m-%d"),
                "title": title_text,
                "url": a_tag["href"], # Note: URL joining happens implicitly in the browser, leaving it as relative/full string here is fine.
                "source": "CTUIL",
                "category": "Update", # Default category for 'Latest News'
                "summary": title_text, # Using title as summary for simplicity
            })
            
    return items


# --- 4. DAILY EXECUTION FUNCTION (Main Entry Point) ---

def run_daily_policy_scraper():
    """
    The main execution function that integrates the scraping logic and pushes results.
    """
    if not db:
        print("FATAL: Skipping publishing as database connection failed.")
        return

    # --- 1. DETERMINE TARGET MONTH (October 2025 as requested for demonstration) ---
    # NOTE: You can change this to datetime.now().year and datetime.now().month for current month
    TARGET_YEAR = 2025 
    TARGET_MONTH = 10 # October
    
    # --- 2. GATHER POLICIES FROM ALL SOURCES ---
    
    # Structure policies by source type as required by the front-end (Central, States, UTs)
    all_policies_to_publish = {
        "central": {},
        "states": {},
        "uts": {}
    }
    
    # --- A. CTUIL Scrape (Live Data) ---
    print(f"STATUS: Running CTUIL scrape for {TARGET_MONTH}/{TARGET_YEAR}...")
    ctuil_policies = _harvest_ctuil_month(TARGET_YEAR, TARGET_MONTH)
    
    # Add results to the publishing dictionary
    if ctuil_policies:
        all_policies_to_publish['central']['CTUIL'] = ctuil_policies
        print(f"SUCCESS: Found {len(ctuil_policies)} policies from CTUIL.")
    else:
        print("INFO: No CTUIL policies found for the target month.")
        
    # --- B. Other Sources (Left Empty as Requested) ---
    # To add CERC, MNRE, Gujarat, etc., you would write functions and call them here.
    
    # --- 3. PUBLISH TO FIRESTORE ---
    total_published = transform_and_publish_policies(all_policies_to_publish)
    
    print(f"\n--- DAILY PUBLISHING COMPLETE ---")
    print(f"RESULT: Successfully published/updated {total_published} documents.")

if __name__ == "__main__":
    run_daily_policy_scraper()
