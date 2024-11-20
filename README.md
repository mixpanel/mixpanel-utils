# Mixpanel-utils Module

### Please note: From v2.0 this module supports Python 3 only. If you require Python 2 use the older mixpanel_api v1.6.5.

### Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Top-level functions](#top-level-functions)
  - [Initialization](#initialization)
  - [Export events](#export-events)
  - [Export people](#export-people)
  - [Import events](#import-events)
  - [Import people](#import-people)
  - [People delete](#people-delete)
  - [Set properties](#set-properties)
  - [Set once properties](#set-once-properties)
  - [Unset properties](#unset-properties)
  - [Increment a property](#increment-a-property)
  - [Append to a property](#append-to-a-property)
  - [Union a property](#union-a-property)
  - [Remove a people property](#remove-a-people-property)
  - [Change a people property name](#change-a-people-property-name)
  - [Deduplicate people profiles](#deduplicate-people-profiles)
  - [Query JQL API](#query-the-jql-api)
  - [Import from Amplitude](#import-from-amplitude)
- [Advanced scripting techniques](#advanced-scripting-techniques)
  - [Lambda functions](#lambda-functions)

NOTE - for a server-side integration with our official Python Library, please refer here: https://github.com/mixpanel/mixpanel-python

The mixpanel-utils module is designed to allow Mixpanel users to rapidly and easily utilize our export APIs to accomplish common tasks such as people/event exports, imports, people profile transform, deletions, etc...

A complete API reference for this module is available here: http://mixpanel-api.readthedocs.io/

#### Installation

You may install the mixpanel-utils module via pip:

```
pip3 install mixpanel-utils
```

#### Usage

To use the mixpanel_utils module import it like so:

```python
from mixpanel_utils import MixpanelUtils
```

Then create a new Mixpanel object like:

```python
mputils = MixpanelUtils(
	'Service Account Secret',
	service_account_username='Service Account Username',
	project_id='Project ID',
	token='Project Token',
)
```

And use the functions below.

Some example scripts are:

\*[mixpanel_utils_example.py](mixpanel_utils_example.py)

#### Top-level functions

These are functions that should allow you to complete a number of tasks with minimal effort.

###### Initialization

```python
__init__(
	api_secret,
	token=None,
	service_account_username=None,
	project_id=None,
	strict_import=True,
	timeout=120,
	pool_size=None,
	read_pool_size=None,
	max_retries=10,
	debug=False,
	residency='us',
)
```

Example:

```python
mputils = MixpanelUtils(
	'ServiceAccountSecretHere',
	token='ProjectTokenHere',
	service_account_username='my-user.12345.mp-service-account',
	project_id=1695321,
)
```

When initializing the Mixpanel class you must specify an API secret as the first parameter, either a Service Account Secret or Project API Secret. If you provide a Service Account secret, you must also provide a `service_account_username` and `project_id`. You may specify a project `token` (this is required if you are importing). You may also specify a `timeout` for request queries (in seconds), the number of CPU cores to use with `pool_size` (defaults to all), the maximum number of simultaneous read connections to make with `read_pool_size`, and the maximum number of retries an import will attempt at a time before giving up.

If your project participates in EU residency, you should specify `residency='eu'` when initializing. If your project participates in India residency, you should specify `residency='in'` when initializing.


###### Export events

```python
export_events(output_file, params, format='json', timezone_offset=None, add_gzip_header=False, compress=False, request_per_day=False, raw_stream=False, buffer_size=1024)

```

Example:

```python
mputils.export_events('event_export.txt',{'from_date':'2016-01-01','to_date':'2016-01-01','event':'["App Install"]'})
```

Exports raw events and writes them to a file using the export endpoint. You must specify the file, the export params (see [here](https://mixpanel.com/help/reference/exporting-raw-data#export-api-reference) for full list of parameters) and the format (default is JSON). Current supported formats are json or csv. You may also add a timezone_offset which should be the offset from UTC the project is in. This modifies the time property so it is in unix time. You can also specify that you wish to receive the files as gzip from our servers using the add_gzip_header option. This is recommended if you believe the export will be large as it can significantly improve transfer time. You may also specify whether you wish to gzip the data after receiving it using the compress option

###### Export people

```python
export_people(self, output_file, params=None, timezone_offset=None, format='json', compress=False)
```

Example:

```python
selector = '(("Albany" in properties["$city"]) and (defined (properties["$city"])))'
parameters = { 'selector' : selector}
mputils.export_people('people_export.txt',parameters,timezone_offset=-8)
```

Exports people profiles and writes them to a file using the engage endpoint. You must specify the file, the export params (see [here](https://mixpanel.com/help/reference/data-export-api#people-analytics) for full list of parameters) and the export format (default is JSON). Current supported formats are JSON or CSV. In addition if you are using behaviors in your parameters you must specify a timezone_offset parameter. See [import_events](#import-events) for information on the timezone_offset parameter. You may also specify whether you wish to gzip the data after receiving it using the compress option.

###### Import events

```python
import_events(data, timezone_offset, dataset_version=None)
```

Example:

```python
mputils.import_events('event_export.txt',timezone_offset=-8)
```

Imports events using the import endpoint. The data parameter is expected to be a filename of a file containing either a CSV or JSON object or list of JSON objects (as in a raw event export) or a list of events. You must specify a timezone offset. This will be the project's timezone offset from UTC. For instance PST is -8 so in that case timezone_offset=-8 would be how you import data that was exported from a project in pacific time during PST time (assuming no timezone_offset was set in the export_events call). The dataset_version is the parameter you must specify if you are importing events into a dataset. See the section on [importing into datasets](#importing-data-into-a-dataset) for more information.

###### Import people

```python
import_people(data, ignore_alias=False, dataset_version=None, raw_record_import=False)
```

Example:

```python
mputils.import_people('people_export.txt')
```

imports people using the engage endpoint. The data parameter is expected to be a filename or a list of objects. The file should be either in CSV or JSON format. The list should be a list of JSON objects (as in an engage export). By default import people checks to see if the distinct_ids specified are aliased. You may specify you wish to ignore alias using ignore_alias=True. If the import is composed of raw engage API updates you may choose to turn on the raw_record_import flag. The dataset_version parameter is for if you wish to import people profiles into a dataset. See the section on [importing into datasets](#importing-data-into-a-dataset) for more information. This method ignores time and IP (so the people profile’s last seen and location will not be updated).

###### People delete

```python
people_delete(profiles=None, query_params=None, timezone_offset=None, ignore_alias=True, backup=True, backup_file=None)
```

Example:

```python
mputils.people_delete(query_params={ 'selector' : '(("Albany" in properties["$city"]) and (defined (properties["$city"])))'})
```

Deletes people profiles using the engage endpoint. You may provide a list of profiles to be deleted or a query parameter (see [here](https://mixpanel.com/help/reference/data-export-api#people-analytics). By default this will create a backup of these profiles with the name backup\_{timestamp}.json where timestamp is the current time in epoch time. You may also provide a backup file name using the backup_file parameter. If your selector is using a behavior you must specify a timezone_offset parameter. This will be the UTC offset of your project time.

###### Set properties

```python
people_set(value, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, timezone_offset=None)
```

Example:

```python
mputils.people_set({'chiles':'green'},query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'}	)
```

Sets people properties to a specific value using the engage endpoint. This should be a dictionary where the keys are the properties you wish to set and the values are the values of those properties. For example, if value was equal to `{ ‘user_level’ : 1 }` it would add the property ‘user*level’ with a value of 1 to all the profiles. You can provide a list of profiles to be deleted or a query parameter (see here for full list of parameters). By default a people_set function call will perform an alias lookup for the distint_id, however by setting the ignore_alias parameter to True it will not perform an alias lookup for the distinct_id. By default this will create a backup of these profiles, however, if you do not want it to you can set the backup property to false to turn off backing up the profiles. The name of this backup profile by default will be name backup*{timestamp}.json where timestamp is the current time in epoch time. You may also provide a backup file name using the backup_file parameter. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Set once properties

```python
people_set_once(value, profiles=None, query_params=None, ignore_alias=False, backup=False,backup_file=None, timezone_offset=None)
```

Example:

```python
mputils.people_set_once({'chiles':'red'},query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```

Sets people properties but only if they do not already exist. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Unset properties

```python
people_unset(value, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, timezone_offset=None)
```

Example:

```python
mputils.people_unset(['coins','feathers'],query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```

Unset a people property on the profiles targeted. In this case value should be a list with a string containing the property to be unset (for example `[‘user_level’]`). See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Increment a property

```python
people_add(value, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, timezone_offset=None)
```

Example:

```python
mputils.people_add({'coins':1},query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```

Adds an amount to a property. Value is a dictionary where the key is the property name you wish to add to and the value is the number you’d like to add to that property (for example if value is `{ ‘user_level’ : 1 }` it would increment the property user_level by 1. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Append to a property

```python
people_append(value, profiles=None, query_params=None, ignore_alias=False, backup=True,backup_file=None, timezone_offset=None)
```

Example:

```python
mputils.people_append({'favorite_colors':'red'},query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```

Appends a value to a list property. Value is a dictionary where the key is the name of the list property and the value is the value to be appended. For example, `{‘Items purchased’ : ‘coffee maker’}` would add the string ‘coffee maker’ to the list property ‘Items purchased’. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Union a property

```python
people_union(value, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, timezone_offset=None)
```

Example:

```python
mputils.people_union({'favorite_colors': ['green']}, query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```

Takes a dictionary containing keys and list values. The list values in the request are merged with the existing list on the user profile, ignoring duplicate list values. For example, `{ ‘Items purchased’: [‘socks’, ‘shirts’] }` will add the values ‘socks’ and ‘shirts’ to the list property ‘Items purchased’ only if they don’t already exist in the list. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Remove a people property

```python
people_remove(value, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, timezone_offset=None)
```

Example:

```python
mputils.people_remove({'favorite_colors':'yellow'},query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```

Takes a dictionary containing keys and values. The value in the request is removed from the existing list on the user profile. If it does not exist, no updates are made. For example, `{ ‘Items purchased’: ‘socks’ }` would remove the value ‘socks’ from the list property ‘Items purchased’. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Change a people property name

```python
people_change_property_name(old_name, new_name, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, unset=True, timezone_offset=0)
```

Example:

```python
mputils.people_change_property_name('favorite_colors', 'best colors',query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```

Renames a property from one name to another. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Deduplicate people profiles

```python
deduplicate_people(profiles=None, prop_to_match='$email', merge_props=False, case_sensitive=False,
                           backup=True, backup_file=None, timezone_offset=0)
```

Example:

```python
mputils.deduplicate_people(prop_to_match='$name',merge_props=True)

```

Deduplicates a set of people profiles, by default all of them, by a property specified by prop_to_match. By default this property is '$email'. This will automatically create a backup of the profiles. You may also have it merge properties together by setting merge_props=True. You may also specify whether the property to match on is case sensitive or not using the case_sensitive parameter. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Query the JQL API

```python
query_jql(script, params=None)
```

Example:

```python
script = '''
function main() {
  return Events({
    from_date: '2016-11-30',
    to_date:   '2016-12-01'
  })
  .groupBy(["name"], mixpanel.reducer.count());
}'''
mputils.query_jql(script)
```

Queries the JQL API. This accepts a script parameter which is a string containing the JQL query you'd like to run (see [here](https://mixpanel.com/help/reference/jql) for information on writing JQL queries). It also accepts a dictionary of global parameters (see [here](https://mixpanel.com/help/reference/jql/api-reference#api/params) for more information on JQL global parameters) by passing this dictionary into the params property. This function will return the JSON response of the JQL query as a python dictionary.

###### Import from Amplitude

```python
import_from_amplitude(amplitude_api_key, amplitude_api_secret, start, end)
```

Example:

```python
# start and end dates are in YYYYMMDDTHH format
mputils.import_from_amplitude("Amplitude Key", "Amplitude Secret", "20210901T00", "20210930T23")
```

Downloads Amplitude project data and imports events and profiles into your Mixpanel project. This accepts your Amplitude project key, Amplitude project secret, a start date, and an end date.

Note:

1. Start and end dates are in `YYYYMMDDTHH` format.
2. start and end date are as per `server_upload_time` as per [Export API doc](https://www.docs.developers.amplitude.com/analytics/apis/export-api/?h=export#considerations).
3. This script would be for projects on [Original ID Merge](https://docs.mixpanel.com/docs/tracking/how-tos/identifying-users#how-does-the-simplified-api-differ-from-the-original-api) only.

#### Advanced scripting techniques

###### Lambda functions

Any of the people operation functions such as people_set accept a value parameter determining what the value of the property will be set to. However, this doesn't just accept a value, it can also accept a function. This is extremely powerful if you want to set a people property to a range of values. Let's walk through an example here.

Say I have a list of distinct_ids and I'd like to add a property "favorite_color" to them with each specific color. This can be done extremely simply with people_set operations as follows:

```python
# we'll provide the list here but this could just as easily be a list in a CSV
profile_list = [{'$distinct_id':'joe@mail.com','favorite_color':'blue'},{'$distinct_id':'george@mail.com','favorite_color':'red'}]
mputils = Mixpanel('secret',token='token')

mputils.people_set(lambda x: {'favorite_color' : x['favorite_color']}, profiles=profile_list)
```

This will iterate over all the dictionary objects in the list profile_list and set the property 'favorite_color' on the people profile in Mixpanel with that distinct_id to that color.

This is extremely useful for any number of things, including renaming properties, adding properties with values from a list or CSV or any other sort of property manipulation.

#### Component functions

These are major functions that you can use to create more complicated scripts than the top level functions may allow.

```python
_write_items_to_csv(items, output_file)
```

This accepts a list of either events or people and writes them to a file as a CSV. The output_file parameter specifies a file like object to be written to. You can create a file like object by doing something like output_file = open('file_to_write_to','wb').

```python
query_export(params)
```

This queries the export endpoint with the provided parameters. This function returns a list of event objects each as their own python dictionary.

```python
query_engage(params={})
```

This queries the engage endpoint with the provided parameters. This function returns a list of people profiles with each profile a python dictionary.
