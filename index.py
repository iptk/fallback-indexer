"""
Searches for new datasets and feed the to the API. This script iterates over 
all datasets found within the path specified by the DATASETS_LOC environment
variable and searches for new datasets. Once a new datasets is found, a new 
metadata set containing the index date is created through the web API instance
at the URL given by the API_ENDPOINT environment variable. This makes sure that
all services connecting to the API receive information about datasets not added
through the API.
"""

from datetime import datetime
import iptk
import requests
import os 

api_endpoint = os.environ.get("API_ENDPOINT", "http://localhost")
datasets_loc = os.environ.get("DATASETS_LOC", "/datasets")

# Internal metadata spec for keeping track of processed datasets
spec_id = "6c8f491fd71276b62fa8e9106dd0659f2baa2e43"

ds = iptk.DatasetStore(datasets_loc)
for d in ds.list_datasets():
	specs = d.metadata_specs()
	if spec_id not in specs:
		# Using the remote API so the change passes through Redis
		metadata = {"index_date": datetime.now().isoformat()}
		res = requests.post(f"{api_endpoint}/v3/datasets/{d.identifier}/meta/{spec_id}", json=metadata)
		print(f"{d.identifier} updated ({res.status_code})")

