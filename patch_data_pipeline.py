#!/usr/bin/env python
"""
Patch data_pipeline.py to call render/result functions directly instead of via HTTP
"""
import re

with open('data_pipeline.py', 'r') as f:
    content = f.read()

# Find the section where RENDER_URL and RESULT_URL are used and replace with direct calls
# We need to import the functions and call them directly

# First, add imports at the top after other imports
import_section = '''from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta, time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import io
from urllib.parse import urljoin
from models import db
from db_storage import store_event_data_to_db
import json
import numpy as np
import time as pytime
import os'''

new_imports = '''from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta, time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import io
from urllib.parse import urljoin
from models import db
from db_storage import store_event_data_to_db
import json
import numpy as np
import time as pytime
import os

# Import render and result functions to call directly
from render import handle_render
from result import handle_result'''

content = content.replace(import_section, new_imports)

# Now replace the HTTP calls with direct function calls
# Find and replace the render_resp = RESILIENT_SESSION.post(RENDER_URL, ...) section
render_http_pattern = r'''render_resp = RESILIENT_SESSION\.post\(\s*RENDER_URL,\s*data=render_payload,\s*timeout=\(10, 30\)\s*\)'''

render_direct_call = '''# Call render function directly instead of HTTP POST
            with app.test_request_context(
                '/render',
                method='POST',
                data=render_payload
            ):
                from flask import request as flask_req
                render_response = handle_render()
                # Extract status and data
                if isinstance(render_response, tuple):
                    render_resp_data, status_code = render_response
                    if status_code != 200:
                        render_resp = type('obj', (object,), {
                            'status_code': status_code,
                            'json': lambda: {'error': 'Render failed'}
                        })()
                    else:
                        render_json = render_resp_data.get_json() if hasattr(render_resp_data, 'get_json') else render_resp_data
                        render_resp = type('obj', (object,), {
                            'status_code': 200,
                            'json': lambda: render_json
                        })()
                else:
                    render_resp = render_response'''

# This is complex because we need app context. Better approach: just skip HTTP and do direct db access

print("Manual patching needed. Use create_file instead.")
