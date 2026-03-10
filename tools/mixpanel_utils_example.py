from mixpanel_utils import MixpanelUtils

if __name__ == '__main__':
	credentials = {
		'service_account_username': '',
		'service_account_password': '',
		'project_id': None,
		'token': '',
	}
	# Optional group context
	group_context = {
		'data_group_id': '',
		'group_key': '',
	}

	# first we are going to make a Mixpanel object instance
	# if you want more information output when running commands set debug=True here
	m = MixpanelUtils(
		service_account_username=credentials['service_account_username'],
		service_account_password=credentials['service_account_password'],
		project_id=credentials['project_id']
	)

	# we'll export as JSON first
	m.export_events('test_event_export.txt',{'from_date':'2017-01-01','to_date':'2017-01-01','event':'["App Store Rating"]'})

	# then as CSV
	m.export_events('test_event_export.csv',{'from_date':'2016-01-02','to_date':'2016-01-02','event':'["App Store RatingInstall"]'}, format='csv')

	# then as gzipped JSON (this is a great way to speed up larger transfers and save space!
	m.export_events('test_event_export_gzip.csv',{'from_date':'2016-01-02','to_date':'2016-01-02', 'event':'["AppApp Store Rating"]'}, add_gzip_header=True)
	# now we're going to export people!
	# let's export people from Albany as JSON now
	selector = '(("Albany" in properties["$city"]) and (defined (properties["$city"])))'

	# we'd add this to our parameters if we were using an event based filter
	# behaviors = r''
	# we'd use this if we wanted to specify the properties that engage would return to us
	# output_properties =

	albany_parameters = {'selector': selector}
	m.export_people('test_people_export.txt', albany_parameters)

	# now export people from Albuquerque as CSV
	selector = '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'
	albq_parameters = {'selector': selector}

	m.export_people('test_people_export.csv', albq_parameters, format='csv')

	# now let's export people based on an event based filter
	# we will export all people who have had a Notification Sent event performed within the last 90 days
	notification_export_parameters = {
										'selector': '(behaviors["behavior_1743"] > 0)',
										'behaviors': r'[{"window": "90d", "name": "behavior_1743", "event_selectors": [{"event": "App Store Rating"}]}]'
									}

	# you need to also supply a timezone_offset parameter if you are exporting people with behaviors. This is the offset from UTC your project time is.
	# for instance if your project is in PST then the offest would be -7
	m.export_people('test_people_export.txt', notification_export_parameters,timezone_offset=-7)


	# now let's import these events and people into a test project
	i = MixpanelUtils(
		service_account_username=credentials['service_account_username'],
		service_account_password=credentials['service_account_password'],
		project_id=credentials['project_id'],
		token=credentials['token']
	)

	# let's import the JSON events first
	# you need to supply a timezone_offset here as well if you did not supply a timezone_offset in the event export
	# this is to ensure that the events are in unix time when they are imported. If you supplied a timezone_offset in the
	# event export you may set timezone_offset to 0. See https://mixpanel.com/help/reference/exporting-raw-data#api-details for more information
	i.import_events('test_event_export.txt', timezone_offset=-7)

	# now let's import the people from Albany
	# people imports never need a timezone_offset
	i.import_people('test_people_export.txt')

	# now import the people from Albuquerque
	i.import_people('test_people_export.csv')

	# now delete all the people from Albany
	i.people_delete(query_params=albany_parameters)

	# now add a property/value {'chiles' : 'green'} to all people from Albuquerque
	i.people_set({'chiles':'green'},query_params=albq_parameters)

	# now try and property/value {'chiles' : 'red'} to all people from Albuquerque
	i.people_set_once({'chiles':'red'},query_params=albq_parameters)

	# now we will add a property 'coins'
	i.people_set({'coins':0},query_params=albq_parameters)

	# and then increment those coins by 1 using add
	i.people_add({'coins':1},query_params=albq_parameters)

	# now remove the coins from everyone from Albuquerque
	i.people_unset(['coins'],query_params=albq_parameters)

	# create a new list property and append 2 items to it
	i.people_append({'favorite_colors':'red'},query_params=albq_parameters)
	i.people_append({'favorite_colors':'green'},query_params=albq_parameters)

	# union green and yellow, only on will show up
	i.people_union({'favorite_colors': ['green']}, query_params=albq_parameters)
	i.people_union({'favorite_colors': ['yellow']}, query_params=albq_parameters)

	# now remove yellow from the property 'favorite_colors'
	i.people_remove({'favorite_colors':'yellow'},query_params=albq_parameters)

	# rename 'favorite_colors' to 'best colors'
	i.people_change_property_name('favorite_colors', 'best colors',query_params=albq_parameters)

	# add a property named 'Revenue' to all targeted people profiles by summing their people transactions
	i.people_revenue_property_from_transactions(query_params=albq_parameters)

	# deduplicates people profiles based on a matching property. See https://mixpanel.com/help/questions/articles/why-does-my-project-have-duplicate-profiles-or-why-are-users-receiving-my-notifications-more-than-once
	# for more information on duplicate profiles.
	# we will deduplicate all people profiles based on the $email property
	i.deduplicate_people(prop_to_match='$email')

	# we will add to each people profile the frequency count of each provided event since the provided date
	# so we will be adding a people property
	i.event_counts_to_people('2017-07-01',['App Install','App Open'])


	# now let's do some group operations
	# first we setup the group context; 
	# group_key is required for importing data
	# data_group_id is required for exporting data
	i.define_group_context(data_group_id=group_context['data_group_id'], group_key=group_context['group_key'])

	# now let's import some groups
	i.import_groups('test_group_import.json')

	# let's download update groups that have the "business" plan to "enterprise"
	# we can pass a query param to download the group profiles that we want to update
	i.group_set({'plan':'enterprise'}, query_params={'where': 'properties["plan"] == "business"'})

	#let's also export group profiles in the "growth" plan
	i.export_groups('test_group_export.json', query_params={'where': 'properties["plan"] == "growth"'})

	# delete group profiles not updated since Jan 1, 2025
	i.group_delete(query_params={
    	"where": 'properties["updated_at"] < datetime("2025-01-01")'
	},backup_file=f'backup_deleted_groups.json')

	# lastly we can even query JQL via the mixpanel api module
	# this is as easy as writing your JQL query and receiving an array of results in return

	#first lets write the JQL query

	query = '''function main() {
				return Events({
					from_date: '2017-08-08',
					to_date:   '2017-08-09'
				})
				.groupBy(["name"], mixpanel.reducer.count());
			}'''

	# then pass it to the function
	results = i.query_jql(query)


