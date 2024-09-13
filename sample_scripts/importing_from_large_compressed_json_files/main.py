import gzip
import glob
import json
from mixpanel_utils import MixpanelUtils
import time

# ----- Configuration
CONFIGURATION = {
    "export_folder": 'exported_files', # folder where the exported files will be saved
	"batch_at": 30_000 # number of events to queue in memory before flushing in parallel threads
}
CREDENTIALS = {
    "project_id": "", # can be left blank when using the API secret instead of service account
	"is_EU_project": False, # set to True if the destination project is in the EU
    "username": "", # service account username; can be left blank when using the API secret
    "password": "" # service account password or API scret
}
# ----- Configuration

mputils = MixpanelUtils(CREDENTIALS["username"], eu=CREDENTIALS['is_EU_project']) if CREDENTIALS['project_id'] == "" else MixpanelUtils(CREDENTIALS["password"], service_account_username=CREDENTIALS['username'],project_id=CREDENTIALS['project_id'],eu=CREDENTIALS['is_EU_project'])

def get_time():
	return int(time.time())

def get_time_desc(value):
	if(value < 120):
		return f'{value} seconds'
	else:
		return f'{round((value/60),2)} mins'

def send_events_and_log():
    global mputils
    global imported_events
    global events_to_import
    global current_count
    global start_time
    mputils.import_events(events_to_import,timezone_offset=0)
    events_to_import = []
    imported_events+= current_count
    print(f'sent {current_count}; total so far: {imported_events} in {get_time_desc(get_time() - start_time)}')
    current_count = 0
	
list_of_files = glob.glob(f"{CONFIGURATION['export_folder']}/*.gz")
list_of_files.sort()
imported_events = 0
events_to_import = []
current_count = 0
start_time = get_time()

for file in list_of_files:
	print(f'reading file {file}')
	with gzip.open(file,'rt') as f:
		for line in f:
			event = json.loads(line)
			events_to_import.append(event)
			current_count+=1
			if(current_count>= CONFIGURATION['batch_at']):
				send_events_and_log()
				
send_events_and_log()
print('done')