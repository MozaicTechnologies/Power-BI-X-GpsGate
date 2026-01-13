#!/usr/bin/env python
import requests
import json

base_url = 'https://omantracking2.com'
token = 'v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=='
app_id = '6'
report_id = 1225

# Get report details
url = f'{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}'
headers = {'Authorization': token}

r = requests.get(url, headers=headers, timeout=10)
print(f'Report 1225 details:')
data = r.json()
print(f'reportFormatId: {data.get("reportFormatId")}')
print(f'Full response:')
print(json.dumps(data, indent=2))
