from mixpanel_api import Mixpanel

if __name__ == '__main__':
	credentials = {
		# You can also use a datasets secret here for any version or import function instead
		# For listing or deleting datasets, however, you must use the project API secret
		'api_secret': '',
		'token': '',
		'dataset_id': '',

	}


	# In order to import a dataset we must first instantiate a Mixpanel object using either your Mixpanel API secret or 
	# a dataset secret. You also need to provide the dataset id you will be importing into as well as the token. 
	# If you don't know it you can set this later. This object can also be used for exporting or importing other data 
	# (see the documentation at http://mixpanel.com/help/reference/importing-datasets for more information)
	m = Mixpanel(
				api_secret = credentials['api_secret'],
				token = credentials['token'], 
				dataset_id = credentials['dataset_id'],
				pool_size=1,
			)


	# You also need to know which dataset version you wish to work with, or create a new one
	# you may retrieve all of the versions for the dataset currently set on the object by
	all_versions = m.list_all_dataset_versions() 

	# you can easily get the latest version by sorting by created_date
	all_versions.sort(key=lambda version: version['created_at'],reverse=True)
	latest_version_id = all_versions[0]['version_id']

	# You can then import events by specifying a file that is JSON formatted with an array of Mixpanel events or a CSV of Mixpanel events
	# see HERE for more information on Mixpanel event objects
	#m.import_events('test_events', timezone_offset=0, dataset_version=latest_version_id)


	# You may also import directly from a python object like this example
	test_events = [
							{'event': 'test_event', 'properties' : { 'distinct_id': '123', 'color': 'blue', 'time': 1502427934000, } },
							{'event': 'test_event', 'properties' : { 'distinct_id': '456', 'color': 'red', 'time': 1502427934050, } },
				]

	m.import_events(test_events,timezone_offset=0,dataset_version=latest_version_id)

	# You can import people in a similar fashion, via a JSON or CSV file, however, you do not need to specify a timezone_offset.
	#m.import_people('people_test', dataset_version=latest_version_id)
	
	# Or a list of people objects. You may see HERE for further explanation on people profiles objects
	test_people = [
							{'$distinct_id': '123', '$properties': { '$email' : 'foo@mail.com'}},
							{'$distinct_id': '456', '$properties': { '$email' : 'bar@mail.com'}},
						]

	m.import_people(test_people, dataset_version=latest_version_id)

	# you may also create a new dataset version if you wish, this will return the current state of the new version
	new_version = m.create_dataset_version()
	if 'error' in new_version:
		print 'seems there was an error creating this dataset version %s' %(new_version['error'])
	else:
		latest_version_id = new_version['version_id']

	# You can also update a version's state. See HERE for more information on the version object
	m.update_dataset_version(latest_version_id,{'writable':True, 'readable': False})

	# You can also specifically change the version state to make it readable. This will make this version queryable 
	# in Mixpanel. There can be only one version that is readable at a time so if another version was set to readable previosuly
	# it will subsequently have it's state changed to reflect this
	m.mark_dataset_version_readable(latest_version_id)

	# let's wait until this version is ready now
	m.wait_until_dataset_version_ready(latest_version_id)
	print 'dataset version ready hit enter to delete the dataset version'
	raw_input()
	# You may also delete a specific dataset version as well
	m.delete_dataset_version(latest_version_id)
