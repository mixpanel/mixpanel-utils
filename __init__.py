import base64
import urllib  # for url encoding
import urllib2  # for sending requests
from httplib import IncompleteRead
from ssl import SSLError
import cStringIO
import logging
import gzip
import shutil
import time
import os
import datetime
from inspect import isfunction
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from paginator import ConcurrentPaginator
from ast import literal_eval
from copy import deepcopy

try:
    import unicodecsv as csv
except ImportError:
    import csv

try:
    import ujson as json
except ImportError:
    import json

try:
    import ciso8601
except ImportError:
    # If ciso8601 is not installed datetime will be used instead
    pass


class Mixpanel(object):
    """An object for querying, importing, exporting and modifying Mixpanel data via their various APIs

    """
    FORMATTED_API = 'https://mixpanel.com/api'
    RAW_API = 'https://data.mixpanel.com/api'
    IMPORT_API = 'https://api.mixpanel.com'
    BETA_IMPORT_API = 'https://api-beta.mixpanel.com'
    VERSION = '2.0'
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.WARNING)
    sh = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    sh.setFormatter(formatter)
    LOGGER.addHandler(sh)

    """
    Public, external methods
    """

    def __init__(self, api_secret, token=None, dataset_id=None, timeout=120, pool_size=None,
                 read_pool_size=2, max_retries=4, debug=False):
        """Initializes the Mixpanel object

        :param api_secret: API Secret for your project
        :param token: Project Token for your project, required for imports
        :param dataset_id: The name of the dataset to operate on
        :param timeout: Time in seconds to wait for HTTP responses
        :param pool_size: Number of threads to use for sending data to Mixpanel (Default value = cpu_count * 2)
        :param read_pool_size: Separate number of threads to use just for read operations (i.e. query_engage)
            (Default value = 2)
        :param max_retries: Maximum number of times to retry when a 5xx HTTP response is received (Default value = 4)
        :param debug: Enable debug logging
        :type api_secret: str
        :type token: str
        :type dataset_id: str
        :type timeout: int
        :type pool_size: int
        :type read_pool_size: int
        :type max_retries: int
        :type debug: bool

        """

        self.api_secret = api_secret
        self.token = token
        self.dataset_id = dataset_id
        self.timeout = timeout
        if pool_size is None:
            # Default number of threads is system dependent
            pool_size = cpu_count() * 2
        self.pool_size = pool_size
        self.read_pool_size = read_pool_size
        self.max_retries = max_retries
        log_level = Mixpanel.LOGGER.getEffectiveLevel()
        ''' The logger is a singleton for the Mixpanel class, so multiple instances of the Mixpanel class will use the
        same logger instance. Subsequent instances can upgrade the logging level to debug but they cannot downgrade it.
        '''
        if debug or log_level == 10:
            Mixpanel.LOGGER.setLevel(logging.DEBUG)
        else:
            Mixpanel.LOGGER.setLevel(logging.WARNING)

    @staticmethod
    def export_data(data, output_file, append_mode=False, format='json', compress=False):
        """Writes and optionally compresses Mixpanel data to disk in json or csv format

        :param data: A list of Mixpanel events or People profiles, if format='json', arbitrary json can be exported
        :param output_file: Name of file to write to
        :param append_mode: Set this to True to append data to an existing file using open() mode 'a+', uses open() mode
            'w+' when False (Default value = False)
        :param format:  Output format can be 'json' or 'csv' (Default value = 'json')
        :param compress:  Option to gzip output (Default value = False)
        :type data: list
        :type output_file: str
        :type append_mode: bool
        :type format: str
        :type compress: bool

        """
        open_mode = 'w+'
        if append_mode:
            open_mode = 'a+'
        with open(output_file, open_mode) as output:
            if format == 'json':
                json.dump(data, output)
            elif format == 'csv':
                Mixpanel._write_items_to_csv(data, output_file)
            else:
                msg = "Invalid format - must be 'json' or 'csv': format = " + str(format) + '\n' \
                      + "Dumping json to " + output_file
                Mixpanel.LOGGER.warning(msg)
                json.dump(data, output)

        if compress:
            Mixpanel._gzip_file(output_file)
            os.remove(output_file)

    @staticmethod
    def sum_transactions(profile):
        """Returns a dict with a single key, 'Revenue' and the sum of all $transaction $amounts for the given profile as
        the value

        :param profile: A Mixpanel People profile dict
        :type profile: dict
        :return: A dict with key 'Revenue' and value containing the sum of all $transactions for the give profile
        :rtype: dict

        """
        total = 0
        try:
            transactions = profile['$properties']['$transactions']
            for t in transactions:
                total = total + t['$amount']
        except KeyError:
            pass
        return {'Revenue': total}

    def request(self, base_url, path_components, params, method='GET', headers=None, raw_stream=False, retries=0):
        """Base method for sending HTTP requests to the various Mixpanel APIs

        :param base_url: Ex: https://api.mixpanel.com
        :param path_components: endpoint path as list of strings
        :param params: dictionary containing the Mixpanel parameters for the API request
        :param method: HTTP method verb: 'GET', 'POST', 'PUT', 'DELETE', 'PATCH'
        :param headers: HTTP request headers dict (Default value = None)
        :param raw_stream: Return the raw file-like response directly from urlopen, only works when base_url is
            Mixpanel.RAW_API
        :param retries: number of times the request has been retried (Default value = 0)
        :type base_url: str
        :type path_components: list
        :type params: dict
        :type method: str
        :type headers: dict
        :type raw_stream: bool
        :type retries: int
        :return: JSON data returned from API
        :rtype: str

        """
        if retries < self.max_retries:
            # Add API version to url path if needed
            if base_url == Mixpanel.IMPORT_API or base_url == Mixpanel.BETA_IMPORT_API:
                base = [base_url]
            else:
                base = [base_url, str(Mixpanel.VERSION)]

            request_url = '/'.join(base + path_components)

            encoded_params = Mixpanel._unicode_urlencode(params)

            # Set up request url and body based on HTTP method and endpoint
            if method == 'GET' or method == 'DELETE':
                data = None
                request_url += '?' + encoded_params
            else:
                data = encoded_params
                if base_url == self.IMPORT_API or 'import-people' in path_components or 'import-events' in path_components:
                    data += '&verbose=1'
                    # Uncomment the line below to log the request body data
                    # Mixpanel.LOGGER.debug(method + ' data: ' + data)
            Mixpanel.LOGGER.debug("Request Method: " + method)
            Mixpanel.LOGGER.debug("Request URL: " + request_url)

            if headers is None:
                headers = {}
            headers['Authorization'] = 'Basic {encoded_secret}'.format(
                encoded_secret=base64.b64encode(self.api_secret + ':'))
            request = urllib2.Request(request_url, data, headers)
            Mixpanel.LOGGER.debug("Request Headers: " + json.dumps(headers))
            # This is the only way to use HTTP methods other than GET or POST with urllib2
            if method != 'GET' and method != 'POST':
                request.get_method = lambda: method

            try:
                response = urllib2.urlopen(request, timeout=self.timeout)
                if raw_stream and base_url == Mixpanel.RAW_API:
                    return response
            except urllib2.HTTPError as e:
                Mixpanel.LOGGER.warning('The server couldn\'t fulfill the request.')
                Mixpanel.LOGGER.warning('Error code: ' + str(e.code))
                Mixpanel.LOGGER.warning('Reason: ' + str(e.reason))
                if hasattr(e, 'read'):
                    Mixpanel.LOGGER.warning('Response: ' + e.read())
                if e.code >= 500:
                    # Retry if we get an HTTP 5xx error
                    Mixpanel.LOGGER.warning("Attempting retry #" + str(retries + 1))
                    self.request(base_url, path_components, params, method=method, headers=headers,
                                 raw_stream=raw_stream, retries=retries + 1)
            except urllib2.URLError as e:
                Mixpanel.LOGGER.warning('We failed to reach a server.')
                Mixpanel.LOGGER.warning('Reason: ' + str(e.reason))
                if hasattr(e, 'read'):
                    Mixpanel.LOGGER.warning('Response: ' + e.read())
                Mixpanel.LOGGER.warning("Attempting retry #" + str(retries + 1))
                self.request(base_url, path_components, params, method=method, headers=headers, raw_stream=raw_stream,
                             retries=retries + 1)
            except SSLError as e:
                if e.message == 'The read operation timed out':
                    Mixpanel.LOGGER.warning('The read operation timed out.')
                    self.timeout = self.timeout + 30
                    Mixpanel.LOGGER.warning(
                        'Increasing timeout to ' + str(self.timeout) + ' and attempting retry #' + str(retries + 1))
                    self.request(base_url, path_components, params, method=method, headers=headers,
                                 raw_stream=raw_stream, retries=retries + 1)
                else:
                    raise

            else:
                try:
                    # If the response is gzipped we go ahead and decompress
                    if response.info().get('Content-Encoding') == 'gzip':
                        buf = cStringIO.StringIO(response.read())
                        f = gzip.GzipFile(fileobj=buf)
                        response_data = f.read()
                    else:
                        response_data = response.read()
                    return response_data
                except IncompleteRead as e:
                    Mixpanel.LOGGER.warning("Response data is incomplete. Attempting retry #" + str(retries + 1))
                    self.request(base_url, path_components, params, method=method, headers=headers,
                                 raw_stream=raw_stream, retries=retries + 1)
        else:
            Mixpanel.LOGGER.warning("Maximum retries reached. Request failed. Try again later.")
            raise BaseException

    def people_operation(self, operation, value, profiles=None, query_params=None, timezone_offset=None,
                         ignore_alias=False, backup=False, backup_file=None):
        """Base method for performing any of the People analytics update operations

        https://mixpanel.com/help/reference/http#update-operations

        :param operation: A string with name of a Mixpanel People operation, like $set or $delete
        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type operation: str
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles operated on
        :rtype: int

        """
        assert self.token, "Project token required for People operation!"
        if profiles is not None and query_params is not None:
            Mixpanel.LOGGER.warning("profiles and query_params both provided, please use one or the other")
            return

        if profiles is not None:
            profiles_list = Mixpanel._list_from_argument(profiles)
        elif query_params is not None:
            profiles_list = self.query_engage(query_params, timezone_offset=timezone_offset)
        else:
            # If both profiles and query_params are None just fetch all profiles
            profiles_list = self.query_engage()

        if backup:
            if backup_file is None:
                backup_file = "backup_" + str(int(time.time())) + ".json"
            self.export_data(profiles_list, backup_file, append_mode=True)

        # Set the dynamic flag to True if value is a function
        dynamic = isfunction(value)

        self._dispatch_batches(self.IMPORT_API, 'engage', profiles_list,
                               [{}, self.token, operation, value, ignore_alias, dynamic])

        profile_count = len(profiles_list)
        Mixpanel.LOGGER.debug(operation + ' operation applied to ' + str(profile_count) + ' profiles')
        return profile_count

    def people_delete(self, profiles=None, query_params=None, timezone_offset=None, ignore_alias=True, backup=True,
                      backup_file=None):
        """Deletes the specified People profiles with the $delete operation and optionally creates a backup file

        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = True)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type profiles: list | str
        :type query_params: dict
        :type ignore_alias: bool
        :type timezone_offset: int | float
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles deleted
        :rtype: int

        """
        return self.people_operation('$delete', '', profiles=profiles, query_params=query_params,
                                     timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                                     backup_file=backup_file)

    def people_set(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False, backup=True,
                   backup_file=None):
        """Sets People properties for the specified profiles using the $set operation and optionally creates a backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles operated on
        :rtype: int

        """
        return self.people_operation('$set', value=value, profiles=profiles, query_params=query_params,
                                     timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                                     backup_file=backup_file)

    def people_set_once(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False,
                        backup=False, backup_file=None):
        """Sets People properties for the specified profiles only if the properties do not yet exist, using the $set_once
        operation and optionally creates a backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles operated on
        :rtype: int

        """
        return self.people_operation('$set_once', value=value, profiles=profiles, query_params=query_params,
                                     timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                                     backup_file=backup_file)

    def people_unset(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False,
                     backup=True, backup_file=None):
        """Unsets properties from the specified profiles using the $unset operation and optionally creates a backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type value: list | (profile) -> list
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles operated on
        :rtype: int

        """
        return self.people_operation('$unset', value=value, profiles=profiles, query_params=query_params,
                                     timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                                     backup_file=backup_file)

    def people_add(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False, backup=True,
                   backup_file=None):
        """Increments numeric properties on the specified profiles using the $add operation and optionally creates a
        backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type value: dict[str, float] | (profile) -> dict[str, float]
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles operated on
        :rtype: int

        """
        return self.people_operation('$add', value=value, profiles=profiles, query_params=query_params,
                                     timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                                     backup_file=backup_file)

    def people_append(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False,
                      backup=True, backup_file=None):
        """Appends values to list properties on the specified profiles using the $append operation and optionally creates
        a backup file.

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type value: dict | (profile) -> dict
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles operated on
        :rtype: int

        """
        return self.people_operation('$append', value=value, profiles=profiles, query_params=query_params,
                                     timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                                     backup_file=backup_file)

    def people_union(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False,
                     backup=True, backup_file=None):
        """Union a list of values with list properties on the specified profiles using the $union operation and optionally
        create a backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type value: dict[str, list] | (profile) -> dict[str, list]
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles operated on
        :rtype: int

        """
        return self.people_operation('$union', value=value, profiles=profiles, query_params=query_params,
                                     timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                                     backup_file=backup_file)

    def people_remove(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False,
                      backup=True, backup_file=None):
        """Removes values from list properties on the specified profiles using the $remove operation and optionally
        creates a backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type value: dict | (profile) -> dict
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles operated on
        :rtype: int

        """
        return self.people_operation('$remove', value=value, profiles=profiles, query_params=query_params,
                                     timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                                     backup_file=backup_file)

    def people_change_property_name(self, old_name, new_name, profiles=None, query_params=None, timezone_offset=None,
                                    ignore_alias=False, backup=True, backup_file=None, unset=True):
        """Copies the value of an existing property into a new property and optionally unsets the existing property.
        Optionally creates a backup file.

        :param old_name: The name of an existing property.
        :param new_name: The new name to replace the old_name with
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. If both query_params and
            profiles are None all profiles with old_name set are targeted. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :param unset:  Option to unset the old_name property (Default value = True)
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :type unset: bool
        :return: Number of profiles operated on
        :rtype: int


        """
        if profiles is None and query_params is None:
            query_params = {'selector': '(defined (properties["' + old_name + '"]))'}
        profile_count = self.people_operation('$set', lambda p: {new_name: p['$properties'][old_name]},
                                              profiles=profiles, query_params=query_params,
                                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                                              backup_file=backup_file)
        if unset:
            self.people_operation('$unset', [old_name], profiles=profiles, query_params=query_params,
                                  timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=False)

        return profile_count

    def people_revenue_property_from_transactions(self, profiles=None, query_params=None, timezone_offset=None,
                                                  ignore_alias=False, backup=True, backup_file=None):
        """Creates a property named 'Revenue' for the specified profiles by summing their $transaction $amounts and
        optionally creates a backup file

        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. If both query_params and
            profiles are None, all profiles with $transactions are targeted. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles operated on
        :rtype: int

        """
        if profiles is None and query_params is None:
            query_params = {'selector': '(defined (properties["$transactions"]))'}

        return self.people_operation('$set', Mixpanel.sum_transactions, profiles=profiles, query_params=query_params,
                                     timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                                     backup_file=backup_file)

    def deduplicate_people(self, profiles=None, prop_to_match='$email', merge_props=False, case_sensitive=False,
                           backup=True, backup_file=None):
        """Determines duplicate profiles based on the value of a specified property. The profile with the latest
        $last_seen is kept and the others are deleted. Optionally adds any properties from the profiles to be deleted to
        the remaining profile using $set_once. Backup files are always created.

        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles. If
            this is None all profiles with prop_to_match set will be downloaded. (Default value = None)
        :param prop_to_match: Name of property whose value will be used to determine duplicates
            (Default value = '$email')
        :param merge_props:  Option to call $set_once on remaining profile with all props from profiles to be deleted.
            This ensures that any properties that existed on the duplicates but not on the remaining profile are
            preserved. (Default value = False)
        :param case_sensitive:  Option to use case sensitive or case insensitive matching (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type profiles: list | str
        :type prop_to_match: str
        :type merge_props: bool
        :type case_sensitive: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles deleted
        :rtype: int

        """
        main_reference = {}
        update_profiles = []
        delete_profiles = []

        if profiles is not None:
            profiles_list = Mixpanel._list_from_argument(profiles)
        else:
            # Unless the user provides a list of profiles we only look at profiles which have the prop_to_match set
            selector = '(boolean(properties["' + prop_to_match + '"]) == true)'
            profiles_list = self.query_engage({'where': selector})

        if backup:
            if backup_file is None:
                backup_file = "backup_" + str(int(time.time())) + ".json"
            self.export_data(profiles_list, backup_file, append_mode=True)

        for profile in profiles_list:
            try:
                match_prop = str(profile["$properties"][prop_to_match])
            except UnicodeError:
                match_prop = profile["$properties"][prop_to_match].encode('utf-8')
            except KeyError:
                continue
            finally:
                try:
                    if not case_sensitive:
                        match_prop = match_prop.lower()
                except NameError:
                    pass

            # Ensure each value for the prop we are matching on has a key pointing to an array in the main_reference
            if not main_reference.get(match_prop):
                main_reference[match_prop] = []

            # Append each profile to the array under the key corresponding to the value it has for prop we are matching
            main_reference[match_prop].append(profile)

        for matching_prop, matching_profiles in main_reference.iteritems():
            if len(matching_profiles) > 1:
                matching_profiles.sort(key=lambda dupe: Mixpanel._dt_from_iso(dupe))
                # We create a $delete update for each duplicate profile and at the same time create a
                # $set_once update for the keeper profile by working through duplicates oldest to newest
                if merge_props:
                    prop_update = {"$distinct_id": matching_profiles[-1]["$distinct_id"], "$properties": {}}
                for x in xrange(len(matching_profiles) - 1):
                    delete_profiles.append({'$distinct_id': matching_profiles[x]['$distinct_id']})
                    if merge_props:
                        prop_update["$properties"].update(matching_profiles[x]["$properties"])
                # Remove $last_seen from any updates to avoid weirdness
                if merge_props and "$last_seen" in prop_update["$properties"]:
                    del prop_update["$properties"]["$last_seen"]
                if merge_props:
                    update_profiles.append(prop_update)

        # The "merge" is really just a $set_once call with all of the properties from the deleted profiles
        if merge_props:
            self.people_operation('$set_once', lambda p: p['$properties'], profiles=update_profiles, ignore_alias=True,
                                  backup=False)

        return self.people_operation('$delete', '', profiles=delete_profiles, ignore_alias=True, backup=False)

    def query_jql(self, script, params=None, format='json'):
        """Query the Mixpanel JQL API

        https://mixpanel.com/help/reference/jql/api-reference#api/access

        :param script: String containing a JQL script to run
        :param params: Optional dict that will be made available to the script as the params global variable.
        :param format: Output format can be either 'json' or 'csv'
        :type script: str
        :type params: dict
        :type format: str
        :return: query output as json or a csv str

        """
        query_params = {"script": script, 'format': format}
        if format == 'csv':
            query_params['download_file'] = 'foo.csv'
        if params is not None:
            query_params["params"] = json.dumps(params)

        response = self.request(Mixpanel.FORMATTED_API, ['jql'], query_params, method='POST')
        if format == 'json':
            return json.loads(response)
        else:
            return response

    def jql_operation(self, jql_script, people_operation, update_value=lambda x: x['value'], jql_params=None,
                      ignore_alias=False, backup=True, backup_file=None):
        """Perform a JQL query to return a JSON array of objects that can then be used to dynamically construct People
            updates via the update_value

        :param jql_script: String containing a JQL script to run. The result should be an array of objects. Those
            objects should contain at least a $distinct_id key
        :param people_operation: A Mixpanel People update operation
        :param update_value: Can be a static value applied to all $distinct_ids or a user defined function or lambda
            that expects a dict containing at least a $distinct_id (or distinct_id) key, as its only parameter and
            returns the value to use for the update on that $distinct_id. The default is a lambda that returns the
            value at the dict's 'value' key.
        :param jql_params: Optional dict that will be made available to the script as the params global variable.
            (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type jql_script: str
        :type people_operation: str
        :type jql_params: dict
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :return: Number of profiles operated on
        :rtype: int

        """

        jql_data = self.query_jql(jql_script, jql_params)

        if backup:
            if backup_file is None:
                backup_file = "backup_" + str(int(time.time())) + ".json"
            # backs up ALL profiles, not just those affected by the JQL since jql_data might not contain full profiles
            self.export_people(backup_file)

        return self.people_operation(people_operation, update_value, profiles=jql_data, ignore_alias=ignore_alias,
                                     backup=False)

    def event_counts_to_people(self, from_date, events):
        """Sets the per user count of events in events list param as People properties

        :param from_date: A datetime or a date string of format YYYY-MM-DD to begin counting from
        :param events: A list of strings of event names to be counted
        :type from_date: datetime | str
        :type events: list[str]
        :return: Number of profiles operated on
        :rtype: int

        """

        jql_script = "function main() { var event_selectors_array = []; _.each(params.events, function(e) {" \
                     "event_selectors_array.push({'event': e});}); return join(Events({from_date: params.from_date," \
                     "to_date: params.to_date, event_selectors: event_selectors_array}), People(), {type: 'inner'})" \
                     ".groupByUser(['event.name'], mixpanel.reducer.count()).map(function(row) {v = {}; v[row.key[1]]" \
                     " = row.value; return {$distinct_id: row.key[0],value: v};});}"

        to_date = datetime.datetime.today().strftime('%Y-%m-%d')

        if isinstance(from_date, datetime.date):
            from_date = from_date.strftime('%Y-%m-%d')

        params = {'from_date': from_date, 'to_date': to_date, 'events': events}
        return self.jql_operation(jql_script, '$set', jql_params=params, backup=False)

    def export_jql_events(self, output_file, from_date, to_date, event_selectors=None, output_properties=None,
                          timezone_offset=0, format='json', compress=False):
        """Export events to disk via JQL. Optional whitelist of properties to include in the output.

        :param output_file: Name of the file to write to
        :param from_date: Date to export events from, can be a datetime object or str of form 'YYYY-MM-DD'
        :param to_date: Date to export events to,, can be a datetime object or str of form 'YYYY-MM-DD'
        :param event_selectors: A single event selector dict or a list of event selector dicts
        :param output_properties: A list of strings of property names to include in the output
        :param timezone_offset: UTC offset in hours of export project timezone setting. If set, used to convert event
            timestamps from project time to UTC
        :param format: Data format for the output can be either 'json' or 'csv'
        :param compress: Optionally gzip the output
        :type output_file: str
        :type from_date: datetime | str
        :type to_date: datetime | str
        :type event_selectors: dict | list[dict]
        :type timezone_offset: int | float
        :type output_properties: list[str]
        :type format: str
        :type compress: bool

        """
        events = self.query_jql_events(from_date=from_date, to_date=to_date, event_selectors=event_selectors,
                                       timezone_offset=timezone_offset, output_properties=output_properties,
                                       format=format)

        self._export_jql_items(events, output_file, format=format, compress=compress)

    def export_jql_people(self, output_file, user_selectors=None, output_properties=None, format='json',
                          compress=False):
        """Export People profiles to disk via JQL by providing a single selector string or a list of selector dicts.
        Optional whitelist of properties to include in the output.

        :param output_file: Name of the file to write to
        :param user_selectors: A selector string or a list of selector dicts
        :param output_properties: A list of strings of property names to include in the output
        :param format: Data format for the output can be 'json' or 'csv'
        :param compress: Optionally gzip the output
        :type output_file: str
        :type user_selectors: str | list[dict]
        :type output_properties: list[str]
        :type format: str
        :type compress: bool

        """
        profiles = self.query_jql_people(user_selectors=user_selectors, output_properties=output_properties,
                                         format=format)

        self._export_jql_items(profiles, output_file=output_file, format=format, compress=compress)

    def query_jql_events(self, from_date, to_date, event_selectors=None, timezone_offset=0, output_properties=None,
                         format='json'):
        """Query JQL for events. Optional whitelist of properties to include in the output.

        :param from_date: Date to export events from, can be a datetime object or str of form 'YYYY-MM-DD'
        :param to_date: Date to export events to,, can be a datetime object or str of form 'YYYY-MM-DD'
        :param event_selectors: A single event selector dict or a list of event selector dicts
        :param timezone_offset: UTC offset in hours of export project timezone setting. If set, used to convert event
            timestamps from project time to UTC
        :param output_properties: A list of strings of property names to include in the output
        :param format: Data format for the output can be either 'json' or 'csv'
        :type from_date: datetime | str
        :type to_date: datetime | str
        :type event_selectors: dict | list[dict]
        :type timezone_offset: int | float
        :type output_properties: list[str]
        :type format: str

        """
        return self._query_jql_items('events', from_date=from_date, to_date=to_date, event_selectors=event_selectors,
                                     output_properties=output_properties, timezone_offset=timezone_offset,
                                     format=format)

    def query_jql_people(self, user_selectors=None, output_properties=None, format='json'):
        """Query JQL for profiles by providing a single selector string or a list of selector dicts.
        Optional whitelist of properties to include in the output.


        :param user_selectors: A selector string or a list of selector dicts
        :param output_properties: A list of strings of property names to include in the output
        :param format: Data format for the output can be 'json' or 'csv'
        :type user_selectors: str | list[dict]
        :type output_properties: list[str]
        :type format: str

        """
        return self._query_jql_items('people', user_selectors=user_selectors, output_properties=output_properties,
                                     format=format)

    def query_export(self, params, add_gzip_header=False, raw_stream=False):
        """Queries the /export API and returns a list of Mixpanel event dicts

        https://mixpanel.com/help/reference/exporting-raw-data#export-api-reference

        :param params: Parameters to use for the /export API request
        :param add_gzip_header: Adds 'Accept-encoding: gzip' to the request headers (Default value = False)
        :param raw_stream: Returns the raw file-like response directly from urlopen instead of creating a list
        :type params: dict
        :type add_gzip_header: bool
        :type raw_stream: bool
        :return: A list of Mixpanel event dicts
        :rtype: list

        """
        headers = {}
        if add_gzip_header:
            headers = {'Accept-encoding': 'gzip'}
        response = self.request(Mixpanel.RAW_API, ['export'], params, headers=headers, raw_stream=raw_stream)
        if response != '':
            if raw_stream:
                return response
            else:
                try:
                    file_like_object = cStringIO.StringIO(response.strip())
                except TypeError as e:
                    Mixpanel.LOGGER.warning('Error querying /export API')
                    return
                raw_data = file_like_object.getvalue().split('\n')
                events = []
                for line in raw_data:
                    events.append(json.loads(line))
                return events
        else:
            Mixpanel.LOGGER.warning('/export API response empty')
            return []

    def query_engage(self, params=None, timezone_offset=None):
        """Queries the /engage API and returns a list of Mixpanel People profile dicts

        https://mixpanel.com/help/reference/data-export-api#people-analytics

        :param params: Parameters to use for the /engage API request. Defaults to returning all profiles.
            (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :type params: dict
        :type timezone_offset: int | float
        :raise RuntimeError: Raises Runtime error if params include behaviors and timezone_offset is None
        :return: A list of Mixpanel People profile dicts
        :rtype: list

        """
        if params is None:
            params = {}
        if 'behaviors' in params and timezone_offset is None:
            raise RuntimeError("timezone_offset required if params include behaviors")
        elif 'behaviors' in params:
            params['as_of_timestamp'] = int(int(time.time()) + (timezone_offset * 3600))

        engage_paginator = ConcurrentPaginator(self._get_engage_page, concurrency=self.read_pool_size)
        return engage_paginator.fetch_all(params)

    def export_events(self, output_file, params, format='json', timezone_offset=None, add_gzip_header=False,
                      compress=False, request_per_day=False, raw_stream=False, buffer_size=1024):
        """Queries the /export API and writes the Mixpanel event data to disk as a JSON or CSV file. Optionally gzip file.

        https://mixpanel.com/help/reference/exporting-raw-data#export-api-reference

        :param output_file: Name of the file to write to
        :param params: Parameters to use for the /export API request
        :param format: Can be either 'json' or 'csv' (Default value = 'json')
        :param timezone_offset: UTC offset in hours of export project timezone setting. If set, used to convert event
            timestamps from project time to UTC
        :param add_gzip_header: Adds 'Accept-encoding: gzip' to the request headers (Default value = False)
        :param compress: Option to gzip output_file (Default value = False)
        :param request_per_day: Option to make one API request (and output file) per day in the exported date range
            (Default value = False)
        :param raw_stream: Option to stream the newline delimited JSON response directly to output_file. If True, format
            , timezone_offset and compress arguments are ignored (Default value = False)
        :param buffer_size: Buffer size in bytes to use if raw_stream is True (Default value = 1024)
        :type output_file: str
        :type params: dict
        :type format: str
        :type timezone_offset: int | float
        :type add_gzip_header: bool
        :type compress: bool
        :type request_per_day: bool
        :type raw_stream: bool
        :type buffer_size: int

        """
        # Increase timeout to 20 minutes if it's still set to default, /export requests can take a long time
        timeout_backup = self.timeout
        if self.timeout == 120:
            self.timeout = 1200

        request_count = 0
        if request_per_day:
            date_format = '%Y-%m-%d'
            f = datetime.datetime.strptime(params['from_date'], date_format)
            t = datetime.datetime.strptime(params['to_date'], date_format)
            delta = t - f
            request_count = delta.days

        for x in xrange(request_count + 1):
            params_copy = deepcopy(params)
            current_file = output_file

            if request_per_day:
                d = time.strptime(params['from_date'], date_format)
                current_day = (datetime.date(d.tm_year, d.tm_mon, d.tm_mday) + datetime.timedelta(x)).strftime(
                    date_format)
                file_components = output_file.split('.')
                current_file = file_components[0] + "_" + current_day
                if len(file_components) > 1:
                    current_file = current_file + '.' + file_components[1]
                params_copy['from_date'] = current_day
                params_copy['to_date'] = current_day

            events = self.query_export(params_copy, add_gzip_header=add_gzip_header, raw_stream=raw_stream)

            if raw_stream:
                if add_gzip_header and current_file[-3:] != '.gz':
                    current_file = current_file + '.gz'
                with open(current_file, 'wb') as fp:
                    shutil.copyfileobj(events, fp, buffer_size)
            else:
                if timezone_offset is not None:
                    # Convert timezone_offset from hours to seconds
                    timezone_offset = timezone_offset * 3600
                    for event in events:
                        event['properties']['time'] = int(event['properties']['time'] - timezone_offset)

                Mixpanel.export_data(events, current_file, format=format, compress=compress)

        # If we modified the default timeout above, restore default setting
        if timeout_backup == 120:
            self.timeout = timeout_backup

    def export_people(self, output_file, params=None, timezone_offset=None, format='json', compress=False):
        """Queries the /engage API and writes the Mixpanel People profile data to disk as a JSON or CSV file. Optionally
        gzip file.

        https://mixpanel.com/help/reference/data-export-api#people-analytics

        :param output_file: Name of the file to write to
        :param params: Parameters to use for the /engage API request (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param format:  (Default value = 'json')
        :param compress:  (Default value = False)
        :type output_file: str
        :type params: dict
        :type timezone_offset: int | float
        :type format: str
        :type compress: bool

        """
        if params is None:
            params = {}
        profiles = self.query_engage(params, timezone_offset=timezone_offset)
        Mixpanel.export_data(profiles, output_file, format=format, compress=compress)

    def import_events(self, data, timezone_offset, dataset_version=None):
        """Imports a list of Mixpanel event dicts or a file containing a JSON array of Mixpanel events.

        https://mixpanel.com/help/reference/importing-old-events

        :param data: A list of Mixpanel event dicts or the name of a file containing a JSON array or CSV of Mixpanel
            events
        :param timezone_offset: UTC offset (number of hours) for the project that exported the data. Used to convert the
            event timestamps back to UTC prior to import.
        :param dataset_version: Dataset version to import into, required if self.dataset_id is set, otherwise
            optional
        :type data: list | str
        :type timezone_offset: int | float
        :type dataset_version: str

        """
        if self.dataset_id or dataset_version:
            if self.dataset_id and dataset_version:
                endpoint = 'import-events'
                base_url = self.BETA_IMPORT_API
            else:
                Mixpanel.LOGGER.warning('Must supply both, dataset_version and dataset_id in init, or neither!')
                return
        else:
            endpoint = 'import'
            base_url = self.IMPORT_API

        self._import_data(data, base_url, endpoint, timezone_offset=timezone_offset, dataset_id=self.dataset_id,
                          dataset_version=dataset_version)

    def import_people(self, data, ignore_alias=False, dataset_version=None, raw_record_import=False):
        """Imports a list of Mixpanel People profile dicts (or raw API update operations) or a file containing a JSON
            array or CSV of Mixpanel People profiles (or raw API update operations).

            https://mixpanel.com/help/reference/http#people-analytics-updates

        :param data: A list of Mixpanel People profile dicts (or /engage API update operations) or the name of a file
            containing a JSON array of Mixpanel People profiles (or /engage API update operations).
        :param ignore_alias: Option to bypass Mixpanel's alias lookup table (Default value = False)
        :param dataset_version: Dataset version to import into, required if self.dataset_id is set, otherwise
            optional
        :param raw_record_import: Set this to True if data is a list of API update operations (Default value = False)
        :type data: list | str
        :type ignore_alias: bool
        :type dataset_version: str

        """
        if self.dataset_id or dataset_version:
            if self.dataset_id and dataset_version:
                endpoint = 'import-people'
                base_url = self.BETA_IMPORT_API
            else:
                Mixpanel.LOGGER.warning('Must supply both, dataset_version and dataset_id in init, or neither!')
                return
        else:
            endpoint = 'engage'
            base_url = self.IMPORT_API

        self._import_data(data, base_url, endpoint, ignore_alias=ignore_alias, dataset_id=self.dataset_id,
                          dataset_version=dataset_version, raw_record_import=raw_record_import)

    def list_all_dataset_versions(self):
        """Returns a list of all dataset version objects for the current dataset_id

        :return: A list of dataset version objects
        :rtype: list

        """
        assert self.dataset_id, 'dataset_id required!'
        return self._datasets_request('GET', dataset_id=self.dataset_id, versions_request=True)

    def create_dataset_version(self):
        """Creates a new dataset version for the current dataset_id. Note that dataset_id must already exist!

        :return: Returns a dataset version object. This version of the dataset is writable by default while the ready
            and readable flags in the version state are false.
        :rtype: dict

        """
        assert self.dataset_id, 'dataset_id required!'
        return self._datasets_request('POST', dataset_id=self.dataset_id, versions_request=True)

    def update_dataset_version(self, version_id, state):
        """Updates a specific version_id for the current dataset_id. Currently, users can only update the state
            of a given dataset version. Only the readable and writable boolean fields can be updated.

        :param version_id: version_id of the version to be updated
        :param state: State object with fields to be updated
        :type version_id: str
        :type state: dict
        :return: True for success, False otherwise.
        :rtype: bool

        """
        assert self.dataset_id, 'dataset_id required!'
        assert 'writable' in state, 'state must specify writable field!'
        assert 'readable' in state, 'state must specify readable field!'
        return self._datasets_request('PATCH', dataset_id=self.dataset_id, versions_request=True,
                                      version_id=version_id, state=state)

    def mark_dataset_version_readable(self, version_id):
        """Updates the readable field of the specified version_id's state to true for the current dataset_id

        :param version_id: version_id of the version to be marked readable
        :type version_id: str
        :return: True for success, False otherwise.
        :rtype: bool

        """
        Mixpanel.LOGGER.debug('Fetching current version...')
        version = self.list_dataset_version(version_id)
        if 'state' in version:
            writable = version['state']['writable']
            state = {'writable': writable, 'readable': True}
            Mixpanel.LOGGER.debug('Updating state with: ' + str(state))
            self.update_dataset_version(version_id, state)
        else:
            Mixpanel.LOGGER.warning('Failed to get dataset version object for version_id ' + str(
                version_id) + 'of dataset ' + self.dataset_id)
            return False

    def delete_dataset_version(self, version_id):
        """Deletes the version with version_id of the current dataset_id

        :param version_id: version_id of the version to be deleted
        :type version_id: str
        :return: True for success, False otherwise.
        :rtype: bool

        """
        assert self.dataset_id, 'dataset_id required!'
        return self._datasets_request('DELETE', dataset_id=self.dataset_id, versions_request=True,
                                      version_id=version_id)

    def list_dataset_version(self, version_id):
        """Returns the dataset version object with the specified version_id for the current dataset_id

        :param version_id: version_id of the dataset version object to return
        :type version_id: str
        :return: Returns a dataset version object
        :rtype: dict

        """
        assert self.dataset_id, 'dataset_id required!'
        return self._datasets_request('GET', dataset_id=self.dataset_id, versions_request=True,
                                      version_id=version_id)

    def wait_until_dataset_version_ready(self, version_id):
        """Polls the ready state of the specified version_id for the current dataset_id every 60 seconds until it's True

        :param version_id: version_id of the dataset_version to check if ready
        :type version_id: str
        :return: Returns True once the dataset version's ready field is true. Returns False if the dataset version isn't
            found
        :rtype: bool

        """
        ready = False
        attempt = 0

        while not ready:
            Mixpanel.LOGGER.debug('Waiting...')
            time.sleep(60)
            version = self.list_dataset_version(version_id)
            attempt = attempt + 1
            if 'state' in version:
                ready = version['state']['ready']
                Mixpanel.LOGGER.debug('Attempt #' + str(attempt) + ': ready = ' + str(ready))
            else:
                Mixpanel.LOGGER.warning('Failed to get dataset version object for version_id ' + str(
                    version_id) + 'of dataset ' + self.dataset_id)
                return False

        return True

    """
    Private, internal methods
    """

    @staticmethod
    def _unicode_urlencode(params):
        """URL encodes a dict of Mixpanel parameters

        :param params: A dict containing Mixpanel parameter names and values
        :type params: dict
        :return: A URL encoded string
        :rtype: str

        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        result = urllib.urlencode([(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params])
        return result

    @staticmethod
    def _response_handler_callback(response):
        """Takes a Mixpanel API response and checks the status

        :param response: A Mixpanel API JSON response
        :type response: str
        :raises RuntimeError: Raises RuntimeError if status is not equal to 1

        """
        response_data = json.loads(response)
        if ('status' in response_data and response_data['status'] != 1) or ('status' not in response_data):
            Mixpanel.LOGGER.warning("Bad API response: " + response)
            raise RuntimeError('Import or Update Failed')
        Mixpanel.LOGGER.debug("API Response: " + response)

    @staticmethod
    def _write_items_to_csv(items, output_file):
        """Writes a list of Mixpanel events or profiles to a csv file

        :param items: A list of Mixpanel events or profiles
        :param output_file: A string containing the path and filename to write to
        :type items: list
        :type output_file: str

        """
        # Determine whether the items are profiles or events based on the presence of a $distinct_id key
        if len(items):
            if '$distinct_id' in items[0]:
                props_key = '$properties'
                initial_header_value = '$distinct_id'
            else:
                props_key = 'properties'
                initial_header_value = 'event'
        else:
            Mixpanel.LOGGER.warning('No data to write!')
            return

        columns = [item[props_key].keys() for item in items]
        subkeys = set([column for props in columns for column in props])
        subkeys = sorted(subkeys)

        # Create the header
        header = [initial_header_value]
        for key in subkeys:
            header.append(key.encode('utf-8'))

        # Create the writer and write the header
        with open(output_file, 'w') as output:
            writer = csv.writer(output)

            writer.writerow(header)

            for item in items:
                row = []
                try:
                    row.append((item[initial_header_value]).encode('utf-8'))
                except KeyError:
                    row.append('')

                for subkey in subkeys:
                    try:
                        row.append((item[props_key][subkey]).encode('utf-8'))
                    except AttributeError:
                        row.append(item[props_key][subkey])
                    except KeyError:
                        row.append('')
                writer.writerow(row)

    @staticmethod
    def _properties_from_csv_row(row, header, ignored_columns):
        """Converts a row from a csv file into a properties dict

        :param row: A list containing the csv row data
        :param header: A list containing the column headers (property names)
        :param ignored_columns: A list of columns (properties) to exclude
        :type row: list
        :type header: list
        :type ignored_columns: list

        """
        props = {}
        for h, prop in enumerate(header):
            # Handle a strange edge case where the length of the row is longer than the length of the header.
            # We do this to prevent an out of range error.
            x = h
            if x > len(row) - 1:
                x = len(row) - 1
            if row[x] == '' or prop in ignored_columns:
                continue
            else:
                try:
                    # We use literal_eval() here to de-stringify numbers, lists and objects in the CSV data
                    p = literal_eval(row[x])
                    props[prop] = p
                except (SyntaxError, ValueError) as e:
                    props[prop] = row[x]
        return props

    @staticmethod
    def _event_object_from_csv_row(row, header, event_index=None, distinct_id_index=None, time_index=None,
                                   time_converter=None):
        """Converts a row from a csv file into a Mixpanel event dict

        :param row: A list containing the Mixpanel event data from a csv row
        :param header: A list containing the csv column headers
        :param event_index: Index of the event name in row list, if None this method will determine the index
            (Default value = None)
        :param distinct_id_index: Index of the distinct_id in row list, if None this method will determine the index
            (Default value = None)
        :param time_index: Index of the time property in row list, if None this method will determine the index
            (Default value = None)
        :param time_converter: A function to convert the value at time_index into a Unix epoch timestamp int in seconds
        :type row: list
        :type header: list
        :type event_index: int
        :type distinct_id_index: int
        :type time_index: int
        :type time_converter: (value) -> int
        :return: A Mixpanel event object
        :rtype: dict

        """
        event_index = (header.index("event") if event_index is None else event_index)
        distinct_id_index = (header.index("distinct_id") if distinct_id_index is None else distinct_id_index)
        time_index = (header.index("time") if time_index is None else time_index)
        time_value = row[time_index]
        timestamp = (int(time_value) if time_converter is None else time_converter(time_value))
        props = {'distinct_id': row[distinct_id_index], 'time': timestamp}
        props.update(Mixpanel._properties_from_csv_row(row, header, ['event', 'distinct_id', 'time']))
        event = {'event': row[event_index], 'properties': props}
        return event

    @staticmethod
    def _people_object_from_csv_row(row, header, distinct_id_index=None):
        """Converts a row from a csv file into a Mixpanel People profile dict

        :param row: A list containing the Mixpanel event data from a csv row
        :param header: A list containing the csv column headers
        :param distinct_id_index: Index of the distinct_id in row list, if None this method will determine the index
            (Default value = None)
        :type row: list
        :type header: list
        :type distinct_id_index: int
        :return: A Mixpanel People profile object
        :rtype: dict

        """
        distinct_id_index = (header.index("$distinct_id") if distinct_id_index is None else distinct_id_index)
        props = Mixpanel._properties_from_csv_row(row, header, ['$distinct_id'])
        profile = {'$distinct_id': row[distinct_id_index], '$properties': props}
        return profile

    @staticmethod
    def _list_from_argument(arg):
        """Returns a list given a string with the path to file of Mixpanel data or an existing list

        :param arg: A string file path or a list
        :type arg: list | str
        :return: A list of Mixpanel events or profiles
        :rtype: list

        """
        item_list = []
        if isinstance(arg, basestring):
            item_list = Mixpanel._list_from_items_filename(arg)
        elif isinstance(arg, list):
            item_list = arg
        else:
            Mixpanel.LOGGER.warning("data parameter must be a string filename or a list of items")

        return item_list

    @staticmethod
    def _list_from_items_filename(filename):
        """Returns a list of Mixpanel events or profiles given the path to a file containing such data

        :param filename: Path to a file containing a JSON array of Mixpanel event or People profile data
        :type filename: str
        :return: A list of Mixpanel events or profiles
        :rtype: list

        """
        item_list = []
        try:
            with open(filename, 'rbU') as item_file:
                # First try loading it as a JSON list
                item_list = json.load(item_file)
        except ValueError as e:
            if e.message == 'No JSON object could be decoded':
                # Based on the error message, try to treat it as CSV
                with open(filename, 'rbU') as item_file:
                    reader = csv.reader(item_file, )
                    header = reader.next()
                    # Determine if the data is events or profiles based on keys in the header.
                    # NOTE: this will fail if it were profile data with a people property named 'event'
                    if 'event' in header:
                        event_index = header.index("event")
                        distinct_id_index = header.index("distinct_id")
                        time_index = header.index("time")
                        for row in reader:
                            event = Mixpanel._event_object_from_csv_row(row, header, event_index, distinct_id_index,
                                                                        time_index)
                            item_list.append(event)
                    elif '$distinct_id' in header:
                        distinct_id_index = header.index("$distinct_id")
                        for row in reader:
                            profile = Mixpanel._people_object_from_csv_row(row, header, distinct_id_index)
                            item_list.append(profile)
                    else:
                        Mixpanel.LOGGER.warning(
                            "Unable to determine Mixpanel data type: CSV header does not contain 'event' or '$distinct_id'")
                        return None
            else:
                # Try treating the file as newline delimited JSON objects
                item_list = []
                with open(filename, 'rbU') as item_file:
                    for item in item_file:
                        item_list.append(json.loads(item))

        except IOError:
            Mixpanel.LOGGER.warning("Error loading data from file: " + filename)

        return item_list

    @staticmethod
    def _gzip_file(filename):
        """gzip an existing file

        :param filename: Path to a file to be gzipped
        :type filename: str

        """
        gzip_filename = filename + '.gz'
        with open(filename, 'rb') as f_in, gzip.open(gzip_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    @staticmethod
    def _prep_event_for_import(event, token, timezone_offset):
        """Takes an event dict and modifies it to meet the Mixpanel /import HTTP spec or dumps it to disk if it is invalid

        :param event: A Mixpanel event dict
        :param token: A Mixpanel project token
        :param timezone_offset: UTC offset (number of hours) of the timezone setting for the project that exported the
            data. Needed to convert the timestamp back to UTC prior to import.
        :type event: dict
        :type token: str
        :type timezone_offset: int | float
        :return: Mixpanel event dict with token added and timestamp adjusted to UTC
        :rtype: dict

        """
        # The /import API requires a 'time' and 'distinct_id' property, if either of those are missing we dump that
        # event to a log of invalid events and return
        if ('time' not in event['properties']) or ('distinct_id' not in event['properties']):
            Mixpanel.LOGGER.warning('Event missing time or distinct_id property, dumping to invalid_events.txt')
            with open('invalid_events.txt', 'a+') as invalid:
                json.dump(event, invalid)
                invalid.write('\n')
                return
        event_copy = deepcopy(event)
        # transforms timestamp to UTC
        event_copy['properties']['time'] = int(int(event['properties']['time']) - (timezone_offset * 3600))
        event_copy['properties']['token'] = token
        return event_copy

    @staticmethod
    def _prep_params_for_profile(profile, token, operation, value, ignore_alias, dynamic):
        """Takes a People profile dict and returns the parameters for an /engage API update

        :param profile: A Mixpanel People profile dict
        :param token: A Mixpanel project token
        :param operation: A Mixpanel /engage API update operation
            https://mixpanel.com/help/reference/http#update-operations
        :param value: The value to use for the operation or a function that takes a People profile and returns the value
            to use
        :param ignore_alias: Option to bypass Mixpanel's alias lookup table
        :param dynamic: Should be set to True if value param is a function, otherwise false.
        :type profile: dict
        :type token: str
        :type value: dict | list | str | (profile) -> dict | (profile) -> list
        :type operation: str
        :type ignore_alias: bool
        :type dynamic: bool
        :return: Parameters for a Mixpanel /engage API update
        :rtype: dict

        """
        # We use a dynamic flag parameter to avoid the overhead of checking the value parameter's type every time
        if dynamic:
            op_value = value(profile)
        else:
            op_value = value

        params = {
            '$ignore_time': True,
            '$ip': 0,
            '$ignore_alias': ignore_alias,
            '$token': token,
            operation: op_value
        }

        try:
            params['$distinct_id'] = profile['$distinct_id']
        except KeyError as e:
            try:
                # If there's no $distinct_id, look for distinct_id instead (could be JQL data)
                params['$distinct_id'] = profile['distinct_id']
            except KeyError as e:
                Mixpanel.LOGGER.warning('Profile object does not contain a distinct id, skipping.')
                return

        return params

    @staticmethod
    def _dt_from_iso(profile):
        """Takes a Mixpanel People profile and returns a datetime object for the $last_seen value or datetime.min if
        $last_seen is not set

        :param profile: A Mixpanel People profile dict
        :type profile: dict
        :return: A datetime representing the $last_seen value for the given profile
        :rtype: datetime

        """
        dt = datetime.datetime.min
        try:
            last_seen = profile["$properties"]["$last_seen"]
            try:
                # Try to use the MUCH faster ciso8601 library, if it's not installed use the built-in datetime library
                dt = ciso8601.parse_datetime_unaware(last_seen)
            except NameError:
                dt = datetime.datetime.strptime(last_seen, "%Y-%m-%dT%H:%M:%S")
        except KeyError:
            return dt
        return dt

    @staticmethod
    def _export_jql_items(items, output_file, format='json', compress=False):
        """Based method for exporting jql events or jql people to disk

        :param items: json list or csv data
        :param output_file: Name of the file to write to
        :param format: Data format for the output can be 'json' or 'csv', should match the data type in items
        :param compress: Optionally gzip the output file
        :type items: list | str
        :type output_file: str
        :type format: str
        :type compress: bool

        """
        if format == 'json':
            Mixpanel.export_data(items, output_file, format=format, compress=compress)
        elif format == 'csv':
            with open(output_file, 'w') as f:
                f.write(items)
            if compress:
                Mixpanel._gzip_file(output_file)
                os.remove(output_file)
        else:
            Mixpanel.LOGGER.warning('Invalid format must be either json or csv, got: ' + format)
            return

    def _get_engage_page(self, params):
        """Fetches and returns the response from an /engage request

        :param params: Query parameters for the /engage API
        :type params: dict
        :return: /engage API response object
        :rtype: dict

        """
        response = self.request(Mixpanel.FORMATTED_API, ['engage'], params)
        data = json.loads(response)
        if 'results' in data:
            return data
        else:
            Mixpanel.LOGGER.warning("Invalid response from /engage: " + response)
            return

    def _dispatch_batches(self, base_url, endpoint, item_list, prep_args, dataset_id=None, dataset_version=None):
        """Asynchronously sends batches of items to the /import, /engage, /import-events or /import-people Mixpanel API
        endpoints

        :param base_url: The base API url
        :param endpoint: Can be 'import', 'engage', '/import-events' or '/import-people'
        :param item_list: List of Mixpanel event data or People updates
        :param prep_args: List of arguments to be provided to the appropriate _prep method in addition to the profile or
            event
        :param dataset_id: Dataset name to import into, required if dataset_version is specified, otherwise optional
        :param dataset_version: Dataset version to import into, required if dataset_id is specified, otherwise
            optional
        :type base_url: str
        :type endpoint: str
        :type item_list: list
        :type prep_args: list
        :type dataset_id: str
        :type dataset_version: str

        """
        pool = ThreadPool(processes=self.pool_size)
        batch = []

        # Decide which _prep function to use based on the endpoint
        if endpoint == 'import' or endpoint == 'import-events':
            prep_function = Mixpanel._prep_event_for_import
        elif endpoint == 'engage' or endpoint == 'import-people':
            prep_function = Mixpanel._prep_params_for_profile
        else:
            Mixpanel.LOGGER.warning(
                'endpoint must be "import", "engage", "import-events" or "import-people", found: ' + str(endpoint))
            return

        if base_url == self.BETA_IMPORT_API:
            batch_size = 1000
        else:
            batch_size = 50

        for item in item_list:
            if prep_args is not None:
                # Insert the given item as the first argument to be passed to the _prep function determined above
                prep_args[0] = item
                params = prep_function(*prep_args)
                if params:
                    batch.append(params)
            else:
                batch.append(item)

            if len(batch) == batch_size:
                # Add an asynchronous call to _send_batch to the thread pool
                pool.apply_async(self._send_batch, args=(base_url, endpoint, batch, dataset_id, dataset_version),
                                 callback=Mixpanel._response_handler_callback)
                batch = []

        # If there are fewer than batch_size updates left ensure one last call is made
        if len(batch):
            # Add an asynchronous call to _send_batch to the thread pool
            pool.apply_async(self._send_batch, args=(base_url, endpoint, batch, dataset_id, dataset_version),
                             callback=Mixpanel._response_handler_callback)
        pool.close()
        pool.join()

    def _send_batch(self, base_url, endpoint, batch, dataset_id=None, dataset_version=None, retries=0):
        """POST a single batch of data to a Mixpanel API and return the response

        :param base_url: The base API url
        :param endpoint: Can be 'import', 'engage', 'import-events' or 'import-people'
        :param batch: List of Mixpanel event data or People updates to import.
        :param dataset_id: Dataset name to import into, required if dataset_version is specified, otherwise optional
        :param dataset_version: Dataset version to import into, required if dataset_id is specified, otherwise
            optional
        :param retries:  Max number of times to retry if we get a HTTP 5xx response (Default value = 0)
        :type base_url: str
        :type endpoint: str
        :type batch: list
        :type dataset_id: str
        :type dataset_version: str
        :type retries: int
        :return: HTTP response from Mixpanel API
        :rtype: str

        """
        try:
            params = {'data': base64.b64encode(json.dumps(batch))}
            if dataset_id:
                params['dataset_id'] = dataset_id
                params['token'] = self.token
            if dataset_version:
                params['dataset_version'] = dataset_version
            response = self.request(base_url, [endpoint], params, 'POST')
            msg = "Sent " + str(len(batch)) + " items on " + time.strftime("%Y-%m-%d %H:%M:%S") + "!"
            Mixpanel.LOGGER.debug(msg)
            return response
        except BaseException:
            Mixpanel.LOGGER.warning("Failed to import batch, dumping to file import_backup.txt")
            with open('import_backup.txt', 'a+') as backup:
                json.dump(batch, backup)
                backup.write('\n')

    def _import_data(self, data, base_url, endpoint, timezone_offset=None, ignore_alias=False, dataset_id=None,
                     dataset_version=None, raw_record_import=False):
        """Base method to import either event data or People profile data as a list of dicts or from a JSON array
        file

        :param data: A list of event or People profile dicts or the name of a file containing a JSON array or CSV of
            events or People profiles
        :param endpoint: can be 'import' or 'engage'
        :param timezone_offset: UTC offset (number of hours) for the project that exported the data. Used to convert the
            event timestamps back to UTC prior to import. (Default value = 0)
        :param ignore_alias: Option to bypass Mixpanel's alias lookup table (Default value = False)
        :param dataset_id: Dataset name to import into, required if dataset_version is specified, otherwise optional
        :param dataset_version: Dataset version to import into, required if dataset_id is specified, otherwise
            optional
        :param raw_record_import: Set this to True if data is a list of People update operations
        :type data: list | str
        :type endpoint: str
        :type timezone_offset: int | float
        :type ignore_alias: bool
        :type dataset_id: str
        :type dataset_version: str
        :type raw_record_import: bool

        """
        assert self.token, "Project token required for import!"
        if self.dataset_id or dataset_version:
            if not (dataset_id and dataset_version):
                Mixpanel.LOGGER.warning('Both dataset_id AND dataset_version are required')
                return

        # Create a list of arguments to be used in one of the _prep functions later
        args = [{}, self.token]

        item_list = Mixpanel._list_from_argument(data)
        if not raw_record_import:
            if endpoint == 'import' or endpoint == 'import-events':
                args.append(timezone_offset)
            elif endpoint == 'engage' or endpoint == 'import-people':
                args.extend(['$set', lambda profile: profile['$properties'], ignore_alias, True])
        else:
            args = None

        self._dispatch_batches(base_url, endpoint, item_list, args, dataset_id=dataset_id,
                               dataset_version=dataset_version)

    def _query_jql_items(self, data_type, from_date=None, to_date=None, event_selectors=None, user_selectors=None,
                         output_properties=None, timezone_offset=0, format='json'):
        """Base method for querying jql for events or People

        :param data_type: Can be either 'users' or 'people'
        :param from_date: Date to query events from, can be a datetime object or str of form 'YYYY-MM-DD'. Only used
        when data_type='events'
        :param to_date: Date to query events to, can be a datetime object or str of form 'YYYY-MM-DD'. Only used when
        data_type='events'
        :param event_selectors: A single event selector dict or a list of event selector dicts. Only used when
        data_type='events
        :param user_selectors: A selector string or a list of selector dicts. Only used when data_type='people'
        :param output_properties:  A list of strings of property names to include in the output
        :param timezone_offset: UTC offset in hours of export project timezone setting. If set, used to convert event
            timestamps from project time to UTC. Only used when data_type='events'
        :param format: Data format for the output can be either 'json' or 'csv'
        :type data_type: str
        :type from_date: datetime | str
        :type to_date: datetime | str
        :type event_selectors: dict | list[dict]
        :type user_selectors: str | list[dict]
        :type output_properties: list[str]
        :type timezone_offset: float | int
        :type format: str

        """

        if data_type == 'events':
            jql_script = "function main() {return Events({from_date: params.from_date,to_date: params.to_date," \
                         "event_selectors: params.event_selectors}).map(function(event) {var result = {event: " \
                         "event.name,properties: {distinct_id: event.distinct_id,time: (event.time / 1000) - " \
                         "(params.timezone_offset * 3600)}};if ('output_properties' in params) {output_properties = " \
                         "params.output_properties;} else {output_properties = Object.keys(event.properties);}" \
                         "_.each(output_properties, prop => result.properties[prop] = event.properties[prop]);return " \
                         "result;});}"

            date_format = '%Y-%m-%d'
            if isinstance(from_date, datetime.datetime):
                from_date = from_date.strftime(date_format)
            if isinstance(to_date, datetime.datetime):
                to_date = to_date.strftime(date_format)
            if event_selectors is None:
                event_selectors = []
            elif isinstance(event_selectors, dict):
                event_selectors = [event_selectors]
            elif isinstance(event_selectors, list):
                pass
            else:
                Mixpanel.LOGGER.warning(
                    'Invalid type for event_selectors, must be dict or list, found: ' + str(type(event_selectors)))

            params = {'from_date': from_date, 'to_date': to_date, 'event_selectors': event_selectors,
                      'timezone_offset': timezone_offset}
        elif data_type == 'people':
            jql_script = "function main() {return People({user_selectors: params.user_selectors}).map(function(user)" \
                         " {var result = {$distinct_id: user.distinct_id,$properties: {}};if ('output_properties' in" \
                         " params) {output_properties = params.output_properties;} else {output_properties = " \
                         "Object.keys(user.properties);}_.each(output_properties, prop => result.$properties[prop]" \
                         " = user.properties[prop]);return result;});}"

            if user_selectors is None:
                user_selectors = []
            elif isinstance(user_selectors, basestring):
                user_selectors = [{'selector': user_selectors}]
            elif isinstance(user_selectors, list):
                pass
            else:
                Mixpanel.LOGGER.warning(
                    'Invalid type for user_selectors, must be str or list, found: ' + str(type(user_selectors)))
                return

            params = {'user_selectors': user_selectors}
        else:
            Mixpanel.LOGGER.warning('Invalid data_type, must be "events" or "people", found: ' + data_type)
            return

        if output_properties is not None:
            params['output_properties'] = output_properties

        return self.query_jql(jql_script, params=params, format=format)

    def _datasets_request(self, method, dataset_id=None, versions_request=False, version_id=None, state=None):
        """Internal base method for making calls to the /datasets API

        :param method: HTTP method verb: 'GET', 'POST', 'PUT', 'DELETE', 'PATCH'
        :param dataset_id: Name of the dataset to operate on
        :param versions_request: Boolean indicating whether or not this request goes to /datasets/versions
        :param version_id: version_id to operate on
        :param state: version state object for use when updating the version
        :type method: str
        :type dataset_id: str
        :type versions_request: bool
        :type version_id: str
        :type state: dict
        :return: Returns the (parsed json) 'data' attribute of the response. Type depends on the specific API call.
            If there is no 'error' and 'data' is None, returns True. Otherwise returns False.

        """
        path = ['datasets']
        if dataset_id is not None:
            path.append(dataset_id)
        if versions_request or version_id is not None:
            if dataset_id is not None:
                path.append('versions')
            else:
                Mixpanel.LOGGER.warning('dataset_id parameter required to make a request to /versions')
                return
            if version_id is not None:
                path.append(str(version_id))
        assert self.token, 'token required for /datasets API calls!'
        arguments = {'token': self.token}
        if state is not None:
            arguments['state'] = json.dumps(state)
        response = self.request(self.BETA_IMPORT_API, path, arguments, method=method)
        Mixpanel.LOGGER.debug('Response: ' + response)
        json_response = json.loads(response)
        if 'error' not in response:
            if 'data' in response:
                data = json_response['data']
                if data is None:
                    return True
                else:
                    return data
            else:
                Mixpanel.LOGGER.warning("Response doesn't contain a data key, got: " + str(response))
                return False
        else:
            Mixpanel.LOGGER.warning('Dataset operation error: ' + response['error'])
            return False
