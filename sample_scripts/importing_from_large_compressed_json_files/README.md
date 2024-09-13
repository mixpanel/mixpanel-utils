# Importing large compressed json files

This base scripts leverages Mixpanel's [import API](https://developer.mixpanel.com/reference/import-events). You're asked to provide a path to a folder that contains a series of comprossed JSONL files (gzip); note the files are not a whole JSON object (array of objects), but instead, each line in the file is a JSON object itself in the format the API expects. Reference the `application/x-ndjson sample` in the link above.

The script will read each file sequentially, queuing into memory up to a threshold defined in the configuration section (by default 30,000 events), at which time it batch send them into our API in parallel threads. By default, the module uses as many threads as available. 

The idea is to avoid loading the whole file into memory, and batch send the data by queuing and flushing.

It can be used in conjuction with the [raw export sample](../exporting_varied_length_date_ranges/) to transfer data from one project to another.

## Configuration

You'll want to edit the configuration section in the script, specially updating the credentials section, as well as the `export_folder` option to point to the right path.