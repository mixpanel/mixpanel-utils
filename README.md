# Mixpanel-api Module

### Table of Contents

* [Installation](#installation)
* [Usage](#usage)
* [Top-level functions](#top-level-functions)
	* [Initialization](#initialization)
	* [Export events](#export-events)
	* [Export people](#export-people)
	* [Import events](#import-events)
	* [Import people](#import-people)
	* [People delete](#people-delete)
	* [Set properties](#set-properties)
	* [Set once properties](#set-once-properties)
	* [Unset properties](#unset-properties)
	* [Increment a property](#increment-a-property)
	* [Append to a property](#append-to-a-property)
	* [Union a property](#union-a-property)
	* [Remove a people property](#remove-a-people-property)
	* [Change a people property name](#change-a-people-property-name)
	* [Deduplicate people profiles](#deduplicate-people-profiles)	
	* [Query JQL API](#query-the-jql-api)
* [Advanced scripting techniques](#advanced-scripting-techniques)
	* [Lambda functions](#lambda-functions)
* [Datasets API](#datasets-api)
	* [Overview](#overview)
	* [Dataset initialization](#datasets-initialization)
	* [Importing data into a dataset](#importing-data-into-a-dataset)
	* [Dataset versions](#dataset-versions)
		* [Listing dataset versions](#listing-datasets-versions)
		* [Creating a new dataset version](#creating-a-new-dataset-version)
		* [Updating a dataset version](#updating-a-dataset-version)
		* [Marking a dataset version readable](#marking-a-dataset-version-readable)
		* [Deleting a dataset version](#deleting-a-dataset-version)
		* [Knowing when a dataset version is ready](#knowing-when-a-dataset-version-is-ready)

NOTE - for a server-side integration with our official Python Library, please refer here: https://github.com/mixpanel/mixpanel-python

The mixpanel-api module is designed to allow Mixpanel users to rapidly and easily utilize our export APIs to accomplish common tasks such as people/event exports, imports, people profile transform, deletions, etc...

A complete API reference for this module is available here: http://mixpanel-api.readthedocs.io/

Please note: At the moment this module supports Python 2 only. Python 3 is not supported.

#### Installation

You may install the mixpanel-api module via pip:

```
pip install mixpanel-api 
```

#### Usage

To use the mixpanel_api module import it like so:

```python
from mixpanel_api import Mixpanel
```
Then create a new Mixpanel object like:
```python
mixpanel = Mixpanel('API Secret', token='Token')
```
And use the functions below.

Some example scripts are: 

*[mixpanel_api_example.py](mixpanel_api_example.py)
*[datasets_exampl.py](datasets_examples.py)
*[dataset_load_example.py](dataset_load_example.py)

#### Top-level functions
These are functions that should allow you to complete a number of tasks with minimal effort.

###### Initialization
```python
__init__(api_secret, token=None, dataset_id=None, timeout=120, pool_size=None, read_pool_size=None, max_retries=10, debug=False)
```
Example:
```python
Mixpanel('secrethere',token='tokenhere')
```
When initializing the Mixpanel class you must specify an api_secret. You may specify a token (this is required if you are importing). You may also specify timeouts for request queries (in seconds), the number of CPU cores to use with pool_size (defaults to all), the maximum number of simultaneous read connections to make with read_pool_size, and the maximum number of retries an import will attempt at a time before giving up.

###### Export events
```python
export_events(output_file, params, format='json', timezone_offset=None, add_gzip_header=False, compress=False, request_per_day=False, raw_stream=False, buffer_size=1024)

```
Example:
```python
export_events('event_export.txt',{'from_date':'2016-01-01','to_date':'2016-01-01','event':'["App Install"]'})
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
m.export_people('people_export.txt',parameters,timezone_offset=-8)
```
Exports people profiles and writes them to a file using the engage endpoint. You must specify the file, the export params (see [here](https://mixpanel.com/help/reference/data-export-api#people-analytics) for full list of parameters) and the export format (default is JSON).  Current supported formats are JSON or CSV. In addition if you are using behaviors in your parameters you must specify a timezone_offset parameter. See [import_events](#import-events) for information on the timezone_offset parameter. You may also specify whether you wish to gzip the data after receiving it using the compress option.

###### Import events
```python
import_events(data, timezone_offset, dataset_version=None)
```
Example:
```python
import_events('event_export.txt',timezone_offset=-8)
```
Imports events using the import endpoint. The data parameter is expected to be a filename of a file containing either a CSV or JSON object or list of JSON objects (as in a raw event export) or a list of events. You must specify a timezone offset. This will be the project's timezone offset from UTC. For instance PST is -8 so in that case timezone_offset=-8 would be how you import data that was exported from a project in pacific time during PST time (assuming no timezone_offset was set in the export_events call). The dataset_version is the parameter you must specify if you are importing events into a dataset. See the section on [importing into datasets](#importing-data-into-a-dataset) for more information.

###### Import people
```python
import_people(data, ignore_alias=False, dataset_version=None, raw_record_import=False)
```
Example:
```python
import_people('people_export.txt')
```
imports people using the engage endpoint. The data parameter is expected to be a filename or a list of objects. The file should be either in CSV or JSON format. The list should be a list of JSON objects (as in an engage export). By default import people checks to see if the distinct_ids specified are aliased. You may specify you wish to ignore alias using ignore_alias=True. If the import is composed of raw engage API updates you may choose to turn on the raw_record_import flag. The dataset_version parameter is for if you wish to import people profiles into a dataset. See the section on [importing into datasets](#importing-data-into-a-dataset) for more information. This method ignores time and IP (so the people profile’s last seen and location will not be updated). 

###### People delete
```python
people_delete(profiles=None, query_params=None, timezone_offset=None, ignore_alias=True, backup=True, backup_file=None)
```
Example:
```python
people_delete(query_params={ 'selector' : '(("Albany" in properties["$city"]) and (defined (properties["$city"])))'})
```
Deletes people profiles using the engage endpoint. You may provide a list of profiles to be deleted or a query parameter (see [here](https://mixpanel.com/help/reference/data-export-api#people-analytics). By default this will create a backup of these profiles with the name backup_{timestamp}.json where timestamp is the current time in epoch time. You may also provide a backup file name using the backup_file parameter. If your selector is using a behavior you must specify a timezone_offset parameter. This will be the UTC offset of your project time.

###### Set properties
```python
people_set(value, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, timezone_offset=None)
```
Example:
```python
people_set({'chiles':'green'},query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'}	)
```
Sets people properties to a specific value using the engage endpoint. This should be a dictionary where the keys are the properties you wish to set and the values are the values of those properties. For example, if value was equal to `{ ‘user_level’ : 1 }` it would add the property ‘user_level’ with a value of 1 to all the profiles. You can provide a list of profiles to be deleted or a query parameter (see here for full list of parameters). By default a people_set function call will  perform an alias lookup for the distint_id, however by setting the ignore_alias parameter to True it will not perform an alias lookup for the distinct_id. By default this will create a backup of these profiles, however, if you do not want it to you can set the backup property to false to turn off backing up the profiles. The name of this backup profile by default will be name backup_{timestamp}.json where timestamp is the current time in epoch time. You may also provide a backup file name using the backup_file parameter. If you are using behaviors in your query_params you must specify a timezone_offset.	

###### Set once properties
```python
people_set_once(value, profiles=None, query_params=None, ignore_alias=False, backup=False,backup_file=None, timezone_offset=None)
```
Example:
```python
people_set_once({'chiles':'red'},query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```
Sets people properties but only if they do not already exist. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Unset properties
```python
people_unset(value, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, timezone_offset=None)
```
Example:
```python
people_unset(['coins','feathers'],query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```
Unset a people property on the profiles targeted. In this case value should be a list with a string containing the property to be unset (for example `[‘user_level’]`). See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Increment a property
```python
people_add(value, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, timezone_offset=None)
```
Example:
```python
people_add({'coins':1},query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```
Adds an amount to a property. Value is a dictionary where the key is the property name you wish to add to and the value is the number you’d like to add to that property (for example if value is `{ ‘user_level’ : 1 }` it would increment the property user_level by 1. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Append to a property
```python
people_append(value, profiles=None, query_params=None, ignore_alias=False, backup=True,backup_file=None, timezone_offset=None)
```
Example:
```python
people_append({'favorite_colors':'red'},query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```
Appends a value to a list property. Value is a dictionary where the key is the name of the list property and the value is the value to be appended. For example, `{‘Items purchased’ : ‘coffee maker’}` would add the string ‘coffee maker’ to the list property ‘Items purchased’. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Union a property
```python
people_union(value, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, timezone_offset=None)
```
Example:
```python
people_union({'favorite_colors': ['green']}, query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```
Takes a dictionary containing keys and list values. The list values in the request are merged with the existing list on the user profile, ignoring duplicate list values. For example, `{ ‘Items purchased’: [‘socks’, ‘shirts’] }` will add the values ‘socks’ and ‘shirts’ to the list property ‘Items purchased’ only if they don’t already exist in the list. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Remove a people property
```python
people_remove(value, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, timezone_offset=None)
```
Example:
```python
people_remove({'favorite_colors':'yellow'},query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```
Takes a dictionary containing keys and values. The value in the request is removed from the existing list on the user profile. If it does not exist, no updates are made. For example, `{ ‘Items purchased’: ‘socks’ }` would remove the value ‘socks’ from the list property ‘Items purchased’. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Change a people property name
```python
people_change_property_name(old_name, new_name, profiles=None, query_params=None, ignore_alias=False, backup=True, backup_file=None, unset=True, timezone_offset=0)
```
Example:
```python
people_change_property_name('favorite_colors', 'best colors',query_params={ 'selector' : '(("Albuquerque" in properties["$city"]) and (defined (properties["$city"])))'})
```
Renames a property from one name to another. See [people_set](#set-properties) for information on the rest of the query parameters. If you are using behaviors in your query_params you must specify a timezone_offset.

###### Deduplicate people profiles
```python
deduplicate_people(profiles=None, prop_to_match='$email', merge_props=False, case_sensitive=False,
                           backup=True, backup_file=None, timezone_offset=0)
```
Example:
```python
deduplicate_people(prop_to_match='$name',merge_props=True)

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
query_jql(script)
```

Queries the JQL API. This accepts a script parameter which is a string containing the JQL query you'd like to run (see [here](https://mixpanel.com/help/reference/jql) for information on writing JQL queries). It also accepts a dictionary of global parameters (see [here](https://mixpanel.com/help/reference/jql/api-reference#api/params) for more information on JQL global parameters) by passing this dictionary into the params property. This function will return the JSON response of the JQL query as a python dictionary. 

#### Advanced scripting techniques

###### Lambda functions
Any of the people operation functions such as people_set accept a value parameter determining what the value of the property will be set to. However, this doesn't just accept a value, it can also accept a function. This is extremely powerful if you want to set a people property to a range of values. Let's walk through an example here. 

Say I have a list of distinct_ids and I'd like to add a property "favorite_color" to them with each specific color. This can be done extremely simply with people_set operations as follows:

```python
# we'll provide the list here but this could just as easily be a list in a CSV
profile_list = [{'$distinct_id':'joe@mail.com','favorite_color':'blue'},{'$distinct_id':'george@mail.com','favorite_color':'red'}]
m = Mixpanel('secret',token='token')

m.people_set(lambda x: {'favorite_color' : x['favorite_color']}, profiles=profile_list)
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

#### Datasets API

#### Overview

The Datasets API allows you to manipulate datasets - additional data from a specific external source. If we don't currently offer a direct connector to a source you wish to use (such as Salesforce) then you may manually upload that data to Mixpanel. Please see our [documentation](www.mixpanel.com/help/reference/importing-datasets#authentication) for more in depth information on Datasets.

##### dataset initialization

In order to use the mixpanel_api module with the Datasets API you must initialize it slightly differently.

```python
dataset = Mixpanel('API Secret','Token',dataset_id='dataset_id')
```

The secret you supply may be either the API secret for the entire project or it may be a dataset specific secret that you may create.

If you use a dataset secret then the dataset that can be used is limited to the one that the secret was provisioned for. In addition you cannot delete a dataset or list all datasets with a dataset secret, you must use the project API secret instead. 

##### Importing data into a dataset

Importing events or people profiles into a dataset may be accomplished by using either the import_events or import_people methods. If you are using these methods for importing to a dataset rather than into your Mixpanel project's data directly you will need to specify the dataset_version parameter as well. See our documentation on [importing people](#import-people) or [importing events](#import-events) for more information. 

Note: If you are importing events that did not come from a Mixpanel export and the time is not shifted by project time, set the timezone_offset parameter to 0.  See [here](https://mixpanel.com/help/reference/exporting-raw-data#api-details) for further explanation on project time shifted data.

Example:
```python
dataset.import_events('new_events',dataset_version=latest_version_id,timezone_offset=0)

##### Dataset versions

A dataset consists of one or more versions. These versions represent the data at different states. Please see our [documentation](http://mixpanel.com/help/reference/importing-datasets#dataset-versions) for more information on dataset versions. You may manipulate the dataset versions using the mixpanel_api module in a variety of ways.

###### Listing dataset versions

```python
list_all_dataset_versions()
```

Example:
```python
current_dataset_versions = dataset.list_all_dataset_versions()
```

You may list all the dataset versions for the dataset that is currently set in the dataset_id parameter on the Mixpanel object. This will return an object that looks like the following example:

```javascript
{
	[
		{
             "created_at": "2017-06-26T23:10:49.386664Z",
             "is_live": false,
             "state": {
                 "readable": false,
                 "readable_at": "0001-01-01T00:00:00Z",
                 "ready": false,
                 "ready_at": "0001-01-01T00:00:00Z",
                 "writable": true
            }
             "version_id": "5631943370604544"

         },
         {
             "created_at": "2017-06-26T23:00:47.617313Z",
             "is_live": false,
             "state": {
                 "readable": false,
                 "readable_at": "0001-01-01T00:00:00Z",
                 "ready": false,
                 "ready_at": "0001-01-01T00:00:00Z",
                 "writable": true
             },
             "version_id": "5764640680181760"
         }
    ]     
}

```

You may also list one specific dataset version. This would be done by:
```python
list_dataset_version(version_id)
```

Example:
```python
dataset.list_dataset_version(latest_version_id)
```
This will return the specified version object of the dataset.


###### Creating a new dataset version

```python
create_dataset_version()
```

Example:
```python
dataset.create_dataset_version()
```

This method will create a new dataset version for the dataset currently set in dataset_id. It will return a dataset version object. This version of the dataset is writable by default while the ready and readable flags in the version state are false. It will return an object that looks like the following:

```javascript
{
    "data": {
        "created_at": "2017-06-26T23:00:47.617313Z",
        "is_live": false,
        "state": {
            "readable": false,
            "readable_at": "0001-01-01T00:00:00Z",
            "ready": false,
            "ready_at": "0001-01-01T00:00:00Z",
            "writable": true
        },
        "version_id": "5764640680181760"
    }
}

```

###### Updating a dataset version

```python
update_dataset_version(version_id, state)
```

Example:
```python
dataset.update_dataset_version(latest_version_id,{"writable": True, "readable": True})
```

This method allows you to update the state object of the dataset version. See our [documentation](http://support-dev.dev.mixpanel.org/help/reference/importing-datasets#dataset-versions) for more information on the state object. This method will return a Boolean for success or failure.

###### Marking a dataset version readable

```python
mark_dataset_version_readable(version_id)
```

Example:
```python
dataset.mark_dataset_version_readable(latest_version)
```

This method is a convenience function wrapping update_dataset_version that marks the provided version_id as readable. Once a version is marked readable it becomes queryable in Mixpanel. This will return a Boolean for success or failure.

###### Deleting a dataset version

```python
delete_dataset_version(version_id)
```

Example:
```python
dataset.delete_dataset_version(latest_version_id)
```

This allows you to delete the specified dataset version. The results will be a Boolean for success or failure.

###### Knowing when a dataset version is ready

```python
wait_until_dataset_version_ready(version_id)
```

Example:
```python
dataset.wait_until_dataset_version_ready(latest_version_id)
# now you can query the latest dataset version
```

This method polls whether the specified dataset version is ready at intervals of 60 seconds. Once a dataset version is in a ready state and the is_live field is set to true, it can be queried in the insights API
