#!/usr/bin/env python
import requests
import json

base_url = 'https://omantracking2.com'
token = 'v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=='
app_id = '6'

# Get all reports
url = f'{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports'
headers = {'Authorization': token}
r = requests.get(url, headers=headers, timeout=10)

reports = r.json()

# Find reports 1225 and 25
for report in reports:
    report_id = report.get('id')
    name = report.get('name')
    fmt_id = report.get('reportFormatId')
    
    if report_id in [1225, 25]:
        print(f'ID: {report_id} | Name: {name} | Format: {fmt_id}')
        params = report.get('parameters', [])
        print(f'  Parameters:')
        for p in params:
            print(f'    - {p.get("parameterName")}')
        print()
