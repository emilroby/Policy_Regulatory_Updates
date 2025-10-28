import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import hashlib
import json
import os
import base64
from typing import Dict, Any, List

# --- 1. SECURE CREDENTIALS AND CONFIGURATION ---
# WARNING: This script is configured to read the base64-encoded key from
# the GitHub Secret named 'FIREBASE_ADMIN_KEY_B64' at runtime.

def initialize_firebase_securely() -> firestore.client | None:
    """Initializes Firebase Admin SDK using a base64-encoded environment secret."""
    try:
        # Get the Base64 encoded key from the environment (e.g., GitHub Secrets)
        # This is where your GitHub Secret variable is securely injected.
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
        print(f"FATAL ERROR: Failed to initialize Firebase. Check GitHub Secret format and name. Details: {e}")
        return None

# Initialize Firestore client globally
db = initialize_firebase_securely()

# --- 2. FIRESTORE PUBLISHING LOGIC ---
APP_ID = "nsefi-policy-tracker"
# This is the exact collection path your live dashboard front-end is listening to.
COLLECTION_PATH = f'artifacts/{APP_ID}/public/data/policies'

def get_unique_document_id(title: str, date_str: str) -> str:
    """Creates a stable, URL-safe ID using a hash of the policy title and date."""
    # Combine key fields
    key_string = f"{title.strip().lower()}-{date_str}"
    
    # Generate a hash for uniqueness and fixed length
    hash_value = hashlib.sha1(key_string.encode('utf-8')).hexdigest()
    
    # Use the publication date and a portion of the hash for the document ID
    return f"{date_str}-{hash_value[:10]}"

def transform_and_publish_policies(policies_snapshot: Dict[str, Any]) -> int:
    """
    Transforms the categorized snapshot data into flat documents and pushes them to Firestore.
    """
    if not db:
        print("ERROR: Firestore client is not available. Cannot publish data.")
        return 0

    policy_list: List[Dict[str, Any]] = []
    
    # 1. Flatten the categorized structure (central, states, uts)
    for source_type, sources in policies_snapshot.items():
        if source_type in ['central', 'states', 'uts']:
            for source_name, items in sources.items():
                for item in items:
                    # Map old structure to the new Firestore document structure
                    document = {
                        "title": item.get('title', 'Untitled Policy'),
                        "url": item.get('url', '#'),
                        "summary": item.get('summary', 'No summary available.'),
                        "source": item.get('source', source_name),
                        "category": item.get('category', 'General'),
                        
                        # Set source_type for front-end categorization
                        "source_type": source_type.capitalize().replace('s', ''), 
                        
                        # Use the standardized date string (YYYY-MM-DD)
                        "publication_date": item.get('date', datetime.utcnow().strftime("%Y-%m-%d")),
                        
                        # Add a timestamp for audit/archiving purposes
                        "published_at": datetime.utcnow().isoformat()
                    }
                    policy_list.append(document)

    print(f"STATUS: Publishing {len(policy_list)} policy documents to {COLLECTION_PATH}...")

    # 3. Perform a Batch Write Operation (Ensures atomicity and efficiency)
    batch = db.batch()
    collection_ref = db.collection(COLLECTION_PATH)
    
    for policy in policy_list:
        # Use the unique ID generator for reliable UPSERT (Update if Exists, Insert if New)
        doc_id = get_unique_document_id(policy['title'], policy['publication_date'])
        doc_ref = collection_ref.document(doc_id)
        
        # We use .set() which creates the document if it doesn't exist or overwrites it if it does.
        batch.set(doc_ref, policy)
    
    batch.commit()
    return len(policy_list)

# --- 3. DAILY EXECUTION FUNCTION (Integrate your Scraping Logic here) ---

def run_daily_policy_scraper():
    """
    The main execution function that runs the scraping logic (to be integrated)
     and pushes the result to Firestore.
    """
    # NOTE: You will replace this mock data with the output of your actual scraping functions
    # (e.g., calling harvest_cerc_month(), harvest_ctuil_month(), etc.).
    mock_data_from_scrapers = {
        "central": {
            "CERC": [
                {"date": "2025-10-27", "title": "New Tariff Policy for Solar Power Procurement Guidelines", "url": "https://example.com/cerc-tariff", "source": "CERC", "category": "Regulation", "summary": "Details of Transmission Access Charge Waiver on Open Access."},
                {"date": "2025-10-26", "title": "CERC Order on Cross-Subsidy Surcharge (CSS) Calculation", "url": "https://example.com/cerc-css", "source": "CERC", "category": "Regulation", "summary": "New formula defined for calculating cross-subsidy surcharge for open access transactions."},
            ],
            "MNRE": [
                 {"date": "2025-10-26", "title": "Guidelines for Green Hydrogen Mission Subsidy", "url": "https://example.com/mnre-hydrogen", "source": "MNRE", "category": "Policy", "summary": "Detailed operational guidelines for availing subsidies under the National Green Hydrogen Mission..."},
            ]
        },
        "states": {
            "Gujarat": [
                {"date": "2025-10-25", "title": "Gujarat EV Charging Infrastructure Policy V2.0", "url": "https://example.com/gujarat-ev", "source": "Gujarat", "category": "Policy", "summary": "New rules simplify land acquisition for solar projects and provide infrastructure subsidies."},
            ]
        },
        "uts": {
            "Delhi": [
                {"date": "2025-10-24", "title": "Delhi Energy Efficiency Building Codes Update", "url": "https://example.com/delhi-codes", "source": "Delhi", "category": "Regulation", "summary": "Mandatory updates to building codes to improve energy efficiency in large commercial complexes."},
            ]
        }
    }
    
    total_published = transform_and_publish_policies(mock_data_from_scrapers)
    
    print(f"\n--- DAILY PUBLISHING COMPLETE ---")
    print(f"RESULT: Successfully published/updated {total_published} documents.")
    print("The front-end dashboard should now be live with this data.")

if __name__ == "__main__":
    run_daily_policy_scraper()
