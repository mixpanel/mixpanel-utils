# Exporting varied length date ranges

This base scripts leverages Mixpanel's [raw export API](https://developer.mixpanel.com/reference/raw-event-export). You can provide a list of objects containing date ranges, and the interval of time that it should be queried for. For example, in the script you can defined for Janauary through March to be queried 30 days at a time, while for April, for each request to be for a single day at a time. That would look like:

```
dates_to_export = [
    {"start": "2024-01-01","end": "2024-03-31", "increment": 30},
    {"start": "2024-04-01","end": "2024-04-30", "increment": 1}
# ]
```

The raw export API has a rate limit of 60 requests per hour, so this can be helpful to export larger date ranges in a single request when there's relatively low amounts of data for those, while exporting smaller date ranges (up to a request per days) when data volumes have increased. Generally speaking, to keep files manageable, each request should be kept under ~1M - 2M events as a rough guideline. During an implementation period, a whole month could be just 100K events (as an example), but likely that's not the case when going into production or as your user count increases.

## Configuration

You'll want to edit the configuration section in the script, specially updating the credentials section, as well as the `dates_to_export` variable.