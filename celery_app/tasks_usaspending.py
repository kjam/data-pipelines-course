import json
import random
import requests

import pandas as pd
from pandas.io.json import json_normalize

from celeryapp import app

@app.task
def get_seti_grant():
    """Return a random SETI Institute grant description."""
    uri = 'https://api.usaspending.gov/api/v1/awards/'
    headers = {'content-type': 'application/json'}
    payload = {
        "limit": 200,
        "fields": ["id", "description", "total_obligation", "type"],
        "filters": [
            {
                "field": "recipient__recipient_unique_id",
                "operation": "equals",
                "value": "137315552"  # DUNS number for the SETI Institute
            },
            {
                "field": "type",
                "operation": "in",
                "value": ["04", "G"]  # 04 = project grants, G = research grants
            }
        ]
    }
    req = requests.post(uri, data=json.dumps(payload), headers=headers)
    if req.status_code == requests.codes.ok:
        details = pd.DataFrame(json_normalize(req.json()['results']))
        return random.choice(details.description.tolist())
    else:
        # if this wasn't a training exercise there'd be actual error handling
        return req.json()
