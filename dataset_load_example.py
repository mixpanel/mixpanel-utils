###########################################################################################################
# This is an example script that demonstrates how you may create a dataset version, import events into it #
# and subsequently make that dataset version ready and alert you as to when it is ready                   #
###########################################################################################################

from mixpanel_api import Mixpanel
import sys

if __name__ == '__main__':
	credentials = {
		# You can also use a datasets secret here for any version or import function instead
		# For listing or deleting datasets, however, you must use the project API secret
		'API_secret': '',
		'dataset_id': '',
		'token': '', 
	}

	event_import_file = ''

	m = Mixpanel(
		credentials['API_secret'],
		token=credentials['token'],
		dataset_id=credentials['dataset_id'],
		pool_size=1,
		)

	result = m.create_dataset_version()
	latest_version = result['version_id']
	
	# print the version id you are using in case you want it in future
	print latest_version

	# You can then import events by specifying a file that is JSON formatted with an array of Mixpanel events or a CSV of Mixpanel events
	# see HERE for more information on Mixpanel event objects
	m.import_events(event_import_file, timezone_offset=0, dataset_version=latest_version)
	print 'done importing'

	m.mark_dataset_version_readable(latest_version)
	# let's wait until this version is ready now
	m.wait_until_dataset_version_ready(latest_version)
	print 'dataset version is ready'

