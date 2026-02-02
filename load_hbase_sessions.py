import json
import happybase
import os
import glob

# -----------------------------
# HBase connection
# -----------------------------
connection = happybase.Connection('localhost', port=9090)
table = connection.table('sessions')

# -----------------------------
# Folder where session files are stored
# -----------------------------
sessions_folder = r'F:\E_commerce_multimodel\Raw_data\data_raw'  # <- Update path if needed
session_files = glob.glob(os.path.join(sessions_folder, 'sessions_*.json'))

# -----------------------------
# Loop through all session files
# -----------------------------
for file_path in session_files:
    print(f"Processing {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        sessions = json.load(f)
    
    for s in sessions:
        # Create row key: user_id + start_time
        row_key = f"{s.get('user_id','unknown')}_{s.get('start_time','unknown')}"
        
        # Build row with safety for missing keys
        row = {
            b'meta:session_id': s.get('session_id','').encode(),
            b'stats:duration': str(s.get('duration_seconds',0)).encode(),
            b'stats:conversion': s.get('conversion_status','none').encode(),
            b'device:type': s.get('device_profile', {}).get('type','unknown').encode(),
            b'device:os': s.get('device_profile', {}).get('os','unknown').encode(),
            b'device:browser': s.get('device_profile', {}).get('browser','unknown').encode(),
            b'geo:country': s.get('geo_data', {}).get('country','unknown').encode(),
            b'geo:state': s.get('geo_data', {}).get('state','unknown').encode(),
            b'geo:city': s.get('geo_data', {}).get('city','unknown').encode(),
            b'geo:ip': s.get('geo_data', {}).get('ip_address','').encode()
        }
        
        # Insert row into HBase
        table.put(row_key, row)

print(" All sessions loaded into HBase successfully!")
