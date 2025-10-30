import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import hashlib
import json
import os
import base64
from typing import Dict, Any, List, Optional
import re
import time

# --- 1. CONFIGURATION AND GITHUB API SETUP ---
# This script is designed to be run by a GitHub Action.

# GitHub Repository Details (Pulled from GitHub Actions Environment Variables)
REPO_OWNER = os.environ.get('GITHUB_REPOSITORY_OWNER')
REPO_NAME = os.environ.get('GITHUB_REPOSITORY').split('/')[-1] if os.environ.get('GITHUB_REPOSITORY') else None
FILE_PATH = "policy_data.json" # The data file our index.html reads
COMMIT_MESSAGE = "Automated policy update: Scraped CTUIL data."

# SECURE TOKEN: Must be set as a GitHub Secret named REPO_ACCESS_TOKEN
GITHUB_TOKEN = os.environ.get('REPO_ACCESS_TOKEN')


# --- 2. DATA PUBLISHING LOGIC (GitHub API) ---

def get_current_file_sha() -> Optional[str]:
    """Retrieves the current SHA of policy_data.json if it exists."""
    if not REPO_OWNER or not REPO_NAME:
        print("ERROR: GITHUB_REPOSITORY_OWNER or GITHUB_REPOSITORY env variables not set.")
        return None
    if not GITHUB_TOKEN:
        print("ERROR: REPO_ACCESS_TOKEN secret is not set.")
        return None
        
    api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FILE_PATH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    
    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            print("Found existing policy_data.json file. Will update.")
            return response.json().get('sha')
        elif response.status_code == 404:
            print("policy_data.json not found. Will create new file.")
            return None # File does not exist yet (first run)
        else:
            print(f"ERROR: Failed to get current SHA. Status: {response.status_code}. Response: {response.text}")
            return None
    except requests.RequestException as e:
        print(f"ERROR: GitHub API Request failed during SHA check. {e}")
        return None

def publish_data_to_github(new_data: Dict[str, Any]) -> bool:
    """Commits the new policy data to the repository as policy_data.json."""
    if not GITHUB_TOKEN or not REPO_OWNER or not REPO_NAME:
        print("FATAL: Missing environment variables (Token, Owner, or Repo). Cannot publish.")
        return False

    api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FILE_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "Content-Type": "application/json",
    }
    
    # 1. Prepare JSON content and Base64 encode it
    json_content = json.dumps(new_data, ensure_ascii=False, indent=2)
    content_b64 = base64.b64encode(json_content.encode('utf-8')).decode('utf-8')
    
    # 2. Get current SHA to overwrite the file (or None for first creation)
    current_sha = get_current_file_sha()
    
    # 3. Construct the commit body
    commit_body = {
        "message": COMMIT_MESSAGE,
        "content": content_b64,
        "committer": {
            "name": "NSEFI Automation Bot",
            "email": "action@github.com"
        }
    }
    # Add SHA only if we are updating an existing file
    if current_sha:
        commit_body["sha"] = current_sha
    
    # 4. Commit the file
    print(f"Attempting to commit {len(new_data.get('policies', []))} policies to {FILE_PATH}...")
    response = requests.put(api_url, headers=headers, data=json.dumps(commit_body))
    
    if response.status_code in (200, 201):
        print(f"SUCCESS: Policy data committed to GitHub. Status: {response.status_code}")
        return True
    else:
        print(f"FATAL ERROR: Failed to commit data. Status: {response.status_code}. Response: {response.text}")
        return False


# --- 3. CTUIL SCRAPING LOGIC ---
# This logic is adapted from your original ctuil.py file

CTUIL_LATEST_URL = "https://ctuil.in/latestnews?p=ajax"
CTUIL_HEADERS = {
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
        # Use d.m.Y format
        return datetime.strptime(raw, '%d.%m.%Y')
    except ValueError:
        try:
            # Fallback for d-m-Y
            return datetime.strptime(raw, '%d-%m-%Y')
        except ValueError:
            print(f"WARN: Date parsing failed for: '{raw}'")
            return None

def _fetch_html(url: str, payload: dict) -> str:
    """Fetches HTML using a POST request."""
    try:
        with requests.Session() as s:
            s.headers.update(CTUIL_HEADERS)
            resp = s.post(url, data=payload, timeout=20)
            resp.raise_for_status()
            return resp.text
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to fetch CTUIL page. Error: {e}")
        return ""

def harvest_ctuil_live(year: int, month: int) -> List[Dict[str, Any]]:
    """Scrapes CTUIL and filters by the requested month."""
    print(f"Requesting data from {CTUIL_LATEST_URL}...")
    payload = {
        'sort_field': 'LatestNews.news_date',
        'sort_type': 'DESC',
        'page': '1',
        'search_keyword': '',
        'from_date': '',
        'to_date': '',
    }
    
    html = _fetch_html(CTUIL_LATEST_URL, payload)
    if not html: 
        print("No HTML returned from CTUIL.")
        return []

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table: 
        print("Could not find data table in CTUIL response.")
        return []

    items: List[Dict[str, Any]] = []
    
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
            policy_id = f"ctuil-{hashlib.sha1(title_text.encode('utf-8')).hexdigest()[:6]}"
            items.append({
                "id": policy_id,
                "publication_date": dt.strftime("%Y-%m-%d"), # Standardized date
                "title": title_text,
                "url": a_tag["href"],
                "source": "CTUIL",
                "category": "Update", # Default category for 'Latest News'
                "summary": title_text, # Using title as summary for simplicity
                "source_type": "Central" # CTUIL is a Central organization
            })
            
    return items


# --- 4. DAILY EXECUTION FUNCTION (Main Entry Point) ---

def run_daily_policy_scraper():
    """
    The main execution function that integrates the scraping logic and commits results.
    """
    
    # --- 1. DETERMINE TARGET MONTH (October 2025 as requested) ---
    # We will use the date from your screenshot for this test.
    TARGET_YEAR = 2025 
    TARGET_MONTH = 10 # October
    
    # --- 2. GATHER POLICIES FROM ALL SOURCES ---
    all_policies = []
    
    # --- A. CTUIL Scrape (Live Data) ---
    print(f"STATUS: Running CTUIL scrape for {TARGET_MONTH}/{TARGET_YEAR}...")
    try:
        ctuil_policies = harvest_ctuil_live(TARGET_YEAR, TARGET_MONTH)
        if ctuil_policies:
            all_policies.extend(ctuil_policies)
            print(f"SUCCESS: Found {len(ctuil_policies)} policies from CTUIL.")
        else:
            print("INFO: No CTUIL policies found for the target month.")
    except Exception as e:
        print(f"ERROR: CTUIL scrape failed. {e}")
        
    # --- B. Other Scrapers (Add them here later) ---
    # e.g., mnre_policies = harvest_mnre(TARGET_YEAR, TARGET_MONTH)
    # all_policies.extend(mnre_policies)
    
    # --- 3. FORMAT DATA FOR FRONTEND ---
    # The frontend is designed to read this exact structure.
    final_data = {
        "policies": all_policies,
        "published_at_utc": datetime.utcnow().isoformat()
    }
    
    # --- 4. PUBLISH TO GITHUB ---
    is_success = publish_data_to_github(final_data)
    
    print(f"\n--- DAILY PUBLISHING COMPLETE ---")
    print(f"RESULT: {'SUCCESS' if is_success else 'FAILURE'}. Total items: {len(all_policies)}")

if __name__ == "__main__":
    if not REPO_OWNER or not REPO_NAME or not GITHUB_TOKEN:
        print("FATAL ERROR: Environment variables (GITHUB_REPOSITORY_OWNER, GITHUB_REPOSITORY, REPO_ACCESS_TOKEN) must be set.")
        print("This script is designed to be run by a GitHub Action.")
    else:
        run_daily_policy_scraper()

