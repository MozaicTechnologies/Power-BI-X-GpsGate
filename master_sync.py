import requests
import time

# --- CONFIGURATION ---
BASE_URL = "http://localhost:5000"  # Change to your Render URL if running remotely
CREDENTIALS = {
    "app_id": "6",
    "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",  # Paste your v2 token
    "base_url": "https://omantracking2.com",
    "tag_id": "39"
}

# --- DEFINE THE TASKS ---
# Add or remove tasks here. 
# Make sure report_id and event_id match your GpsGate setup.
TASKS = [
    {
        "name": "Speeding Data",
        "endpoint": "/speeding-data",
        "payload": {**CREDENTIALS, "report_id": "25", "event_id": "18"}
    },
    {
        "name": "Trip Data",
        "endpoint": "/trip-data",
        "payload": {**CREDENTIALS, "report_id": "25"} # Replace with your Trip Report ID
    },
    {
        "name": "Idle Data",
        "endpoint": "/idle-data",
        "payload": {**CREDENTIALS, "report_id": "25", "event_id": "1328"} # Replace with yours
    }
]

def run_sync():
    print("🚀 Starting Full Fleet Data Sync...")
    print("-" * 40)
    
    for task in TASKS:
        print(f"📡 Syncing {task['name']}...")
        try:
            # We use a long timeout (300s) because these reports can be heavy
            response = requests.post(
                f"{BASE_URL}{task['endpoint']}", 
                json=task['payload'], 
                timeout=300 
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Success: {result.get('message')} | Rows: {result.get('rows_inserted')}")
            else:
                print(f"❌ Failed: {task['name']} returned {response.status_code}")
                print(f"   Detail: {response.text}")
                
        except Exception as e:
            print(f"⚠️ Error syncing {task['name']}: {e}")
        
        # Short pause to prevent API rate limiting
        print("Waiting 5 seconds before next task...")
        time.sleep(5)

    print("-" * 40)
    print("🏁 Full Sync Complete!")

if __name__ == "__main__":
    run_sync()