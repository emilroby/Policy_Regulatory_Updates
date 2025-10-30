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
# WARNING: This script MUST be run by a GitHub Action that has write permissions (REPO_ACCESS_TOKEN).

# GitHub Repository Details (Pulled from GitHub Actions Environment Variables)
# These variables are automatically provided by GitHub Actions at runtime.
REPO_OWNER = os.environ.get('GITHUB_REPOSITORY_OWNER')
REPO_NAME = os.environ.get('GITHUB_REPOSITORY').split('/')[-1] if os.environ.get('GITHUB_REPOSITORY') else "nsefi-policy-tracker"
FILE_PATH = "policy_data.json"
COMMIT_MESSAGE = "Automated policy update: Scraped CTUIL data."

# SECURE TOKEN: Must be set as a GitHub Secret (REPO_ACCESS_TOKEN)
GITHUB_TOKEN = os.environ.get('REPO_ACCESS_TOKEN')


# --- 2. DATA PUBLISHING LOGIC (GitHub API) ---

def get_current_file_sha() -> Optional[str]:
    """Retrieves the current SHA of the file if it exists, used for update commit."""
    if not GITHUB_TOKEN:
        print("ERROR: GITHUB_TOKEN (REPO_ACCESS_TOKEN) is not set.")
        return None
        
    api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FILE_PATH}"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    
    try:
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            return response.json().get('sha')
        elif response.status_code == 404:
            return None # File does not exist yet (first run)
        else:
            print(f"ERROR: Failed to get current SHA. Status: {response.status_code}. Response: {response.text}")
            return None
    except requests.RequestException as e:
        print(f"ERROR: GitHub API Request failed during SHA check. {e}")
        return None

def publish_data_to_github(new_data: Dict[str, Any]) -> bool:
    """Commits the new policy data to the repository as policy_data.json."""
    if not GITHUB_TOKEN:
        print("FATAL: GITHUB_TOKEN (REPO_ACCESS_TOKEN) is required for publishing.")
        return False

    api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FILE_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
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
        "sha": current_sha # Required to update existing files
    }
    
    # 4. Commit the file
    response = requests.put(api_url, headers=headers, data=json.dumps(commit_body))
    
    if response.status_code in (200, 201):
        print(f"SUCCESS: Policy data committed to GitHub. Status: {response.status_code}")
        return True
    else:
        print(f"FATAL ERROR: Failed to commit data. Status: {response.status_code}. Response: {response.text}")
        return False


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
        # Note: The strptime format needs to be robust for the date format on the site
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
    except requests.RequestException as e:
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
                "url": a_tag["href"],
                "source": "CTUIL",
                "category": "Update",
                "summary": title_text,
            })
            
    return items


# --- 4. DAILY EXECUTION FUNCTION (Main Entry Point) ---

def run_daily_policy_scraper():
    """
    The main execution function that integrates the scraping logic and commits results.
    """
    # --- 1. DETERMINE TARGET MONTH (October 2025 as requested for demonstration) ---
    # NOTE: Change these if you want to scrape the current month instead of 2025/10.
    TARGET_YEAR = 2025 
    TARGET_MONTH = 10 
    
    # --- 2. GATHER POLICIES FROM ALL SOURCES ---
    all_policies = []
    
    # --- A. CTUIL Scrape (Live Data) ---
    print(f"STATUS: Running CTUIL scrape for {TARGET_MONTH}/{TARGET_YEAR}...")
    ctuil_policies = _harvest_ctuil_month(TARGET_YEAR, TARGET_MONTH)
    
    if ctuil_policies:
        all_policies.extend(ctuil_policies)
        print(f"SUCCESS: Found {len(ctuil_policies)} policies from CTUIL.")
    else:
        print("INFO: No CTUIL policies found for the target month.")
        
    # --- B. Other Sources (Left Empty as Requested) ---
    # NOTE: Add other scraping calls here (e.g., harvest_mnre(), harvest_gujarat())
    
    # --- 3. FORMAT DATA FOR FRONTEND ---
    # Policies are now a flat list, but we organize them for the JSON file structure.
    final_data = {
        "policies": all_policies,
        "published_at_utc": datetime.utcnow().isoformat()
    }
    
    # --- 4. PUBLISH TO GITHUB ---
    is_success = publish_data_to_github(final_data)
    
    print(f"\n--- DAILY PUBLISHING COMPLETE ---")
    print(f"RESULT: {'SUCCESS' if is_success else 'FAILURE'}. Total items: {len(all_policies)}")

if __name__ == "__main__":
    run_daily_policy_scraper()
