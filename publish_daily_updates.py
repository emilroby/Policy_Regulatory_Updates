import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
from typing import Dict, Any, List
import hashlib
import json

# --- 1. SECURE CREDENTIALS AND CONFIGURATION ---
# WARNING: This JSON content contains your private key. 
# DO NOT COMMIT THIS FILE TO A PUBLIC GITHUB REPOSITORY.

# This dictionary contains the content of your uploaded JSON Admin SDK key.
ADMIN_SDK_JSON_CONTENT = {
  "type": "service_account",
  "project_id": "nsefi-policy-tracker",
  "private_key_id": "820276cfc26c0621199f7feee643342ba9b7577e",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC0gNU005ZE0RrJ\nqHQy7BJa7A3Wafolrf5PenrpEw6zqmnv83hxt/K4XhSLNoUwCF2PzUdxaxaB5ESz\nY0Q6PldexM7JkDCc0NGG4d4Ywv3eN44jXIn2sxdZuVRuOxxZjhYLKc5jQ3S/qwzy\ni71yHgjL9kbgmJRpZtYVrlhE7rZKGwaMrkUhAW0p5IyCu9JtaGzCeGVk1pBpDm2k\n6VVu4x5PU7tQNJ1LHiA9aO9Sxk1KdeTmiZCCEPNUjbyl+AY+ZOgr0VGPzNOttBa2\nPjYjUSMGjGhOBayRiKBe9ePiorxGWJLWBZpDywfxmb0wkDu47J1yxRTYsPQ0DE1q\nPMJRQ3/5AgMBAAECggEAB9S4Cz7ru8gToLCTe/sUb8ficM0vuk9CY3nVl8OJJX/M\n8UlPjiV0HQ4N151RqWwPfRPjl9bUxuVbmqnBlcJLIZQpwYJlLYR8tpyZeVsKTwf7\nfWrAHIkYB/87m72qRPNeXvUdHF7ak48s9F/eI/OMH6crW4abF7iLrKyf7TurAmt3\nk2EVatgn54ES6sSsi1sdH05CGSDZ+2ti+KwmuDezpxju9KrHKzTsTkVmEdbHV+GU\nBir6T9eP1JIixUrRP2P5R2ltgsSitciVnuvBIhprTW6boT7ztLgHFPPl/iO9S8PP\ncTtJMyUp0/tpHvjs7KLKkuXjCA6ojk3wK0rmML+ZxQKBgQDeU7e3gu882gAd1hBg\nmJZ3Yz6VeavBGSYSsXFhbK4y2uhPGxMpbmVXYZQsctbx2ebiMZ0Hvy6AHVuAuJiw\n0h8C+C/72wQlLFBYbo3V7vrkw7BlfuKzTa6J68YGK4P1N072jRPA7yX5ATasbx3Q\nsPU7Q2GOS/p3AkSgH3faXpFmvQKBgQDP13uaNOw85yhlPBd8AL6Oqyk1eA70p0dV\nHaPmvWcD0/yr4LL9sF6zXgHibyGauMxsxYkoJPg5Q6tk7GWFK1X5bOuwwwTrPkMu\nQ5p94nzgv29OqBkNJeSvYYTSWDjTqeSb2n4oEy1VPtVb8qA775G2Q2z8ZETOXevr\nZbq19rif7QKBgG7vPQNvbOpjKJ26m29nk+S8e5TgIih24P2A1r8zGHS9sB8Qtm38\n7Mo+IU5QexowjTkeYmlkJtK8U1UWRvIr1leH+YFlFltqEikd+N3fogcV8eWi+4FW\naJnfMG8RtYVc9KSnXkztx3fI+DvwMeNY+PR6OapkFPTfB9kR+p7WgxzNAoGAbKxT\n4YaIezO2iHBKKzlMadaO/nkfAMcyYgvUdkJUScke1VPw4vrEGW8u9xF6dabopHsI\nwyfJbk+2n1eHoYDOPFO6TLs7qDDu+ZK5hdbVysHt1ifIqXpFv3ny8/TTCcFMWj19\nN0EHtAndj20mYRBblPxeUP2wiGLNh1CytpAhauUCgYB8IEGGZyb629K4XCJ5Y0na\ngNxoHlHgJ98jr17AEbsGkJ/nG3DgU+KkiBMnZgvEAD1+7A6CiuowBb8KocMp85iS\nBEMn2qOADE+DmXwSRCV4ndp4hG6qWvT2gBRXGlctKdw0ba4xVxWmFlMRKsXpxuXi\ncrlCcCe82XRiAAsVjHgnng==\n-----END PRIVATE KEY-----\n",
  "client_email": "firebase-adminsdk-fbsvc@nsefi-policy-tracker.iam.gserviceaccount.com",
  "client_id": "118118860853835490307",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40nsefi-policy-tracker.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# Initialize Firebase Admin SDK
try:
    # Use credentials.Certificate to load the JSON content directly
    cred = credentials.Certificate(ADMIN_SDK_JSON_CONTENT)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("STATUS: Firebase Admin SDK Initialized Successfully.")
except Exception as e:
    print(f"FATAL ERROR: Failed to initialize Firebase. Details: {e}")
    # Setting db to None ensures the rest of the script safely handles the failure
    db = None

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
    
    # Use a portion of the hash for the document ID
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
            # sources is a dictionary of {organization_name: [list_of_items]}
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

# --- 4. DAILY EXECUTION FUNCTION (Integrate your Scraping Logic here) ---

def run_daily_policy_scraper():
    """
    The main execution function that simulates running your scraping logic 
    and pushes the result to Firestore.
    """
    if not db:
        print("FATAL: Skipping publishing as database connection failed.")
        return

    # --- SIMULATE DATA GATHERING FROM SCRAPERS (REPLACE THIS SECTION) ---
    # This mock data must be replaced by calling your actual Python scraping functions 
    # (e.g., from your old project: harvest_cerc_month(), harvest_ctuil_month(), etc.)
    
    # The structure MUST be: {source_type: {source_name: [list of policies]}}
    mock_data_from_scrapers = {
        "central": {
            "CERC": [
                {"date": "2025-10-27", "title": "New Tariff Policy for Solar Power Procurement Guidelines", "url": "https://example.com/cerc-tariff", "source": "CERC", "category": "Regulation", "summary": "Details of Transmission Access Charge Waiver on Open Access..."},
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
    
    # --- END SIMULATION ---

    total_published = transform_and_publish_policies(mock_data_from_scrapers)
    
    print(f"\n--- DAILY PUBLISHING COMPLETE ---")
    print(f"RESULT: Successfully published/updated {total_published} documents.")
    print("The front-end dashboard should now be live with this data.")

if __name__ == "__main__":
    run_daily_policy_scraper()
