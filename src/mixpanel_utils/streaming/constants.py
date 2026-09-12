"""Shared constants for the streaming pipeline."""

# Mixpanel API base URLs by region
BASE_URLS = {
    "US": "https://api.mixpanel.com",
    "EU": "https://api-eu.mixpanel.com",
    "IN": "https://api-in.mixpanel.com",
}

# Export API base URLs by region
EXPORT_BASE_URLS = {
    "US": "https://data.mixpanel.com",
    "EU": "https://data-eu.mixpanel.com",
    "IN": "https://data-in.mixpanel.com",
}

# Profile export/engage base URLs
ENGAGE_BASE_URLS = {
    "US": "https://mixpanel.com",
    "EU": "https://eu.mixpanel.com",
    "IN": "https://in.mixpanel.com",
}

# API paths by record type
API_PATHS = {
    "event": "/import",
    "user": "/engage",
    "group": "/groups",
    "export": "/api/2.0/export",
    "profile-export": "/api/2.0/engage",
    "profile-delete": "/api/2.0/engage",
    "group-export": "/api/2.0/engage",
    "group-delete": "/api/2.0/engage",
}

# HTTP methods by record type
HTTP_METHODS = {
    "event": "POST",
    "user": "POST",
    "group": "POST",
    "export": "GET",
    "profile-export": "POST",
    "profile-delete": "POST",
    "group-export": "POST",
    "group-delete": "POST",
}

# Record types that use the export base URL
EXPORT_RECORD_TYPES = {"export", "export-import-event"}

# Record types that use the engage base URL
ENGAGE_RECORD_TYPES = {
    "profile-export", "profile-delete",
    "group-export", "group-delete",
    "export-import-profile", "export-import-group",
}

# Record types that support gzip compression on requests
GZIP_RECORD_TYPES = {"event", "export-import-event"}

# Status codes that trigger retry
RETRY_STATUS_CODES = {429, 500, 501, 502, 503, 504, 508, 524}

# Batch limits
MAX_RECORDS_PER_BATCH = 2000
MAX_BYTES_PER_BATCH = int(9.8 * 1024 * 1024)  # 9.8 MB
DEFAULT_WORKERS = 10
DEFAULT_MAX_RETRIES = 10
DEFAULT_COMPRESSION_LEVEL = 6

# Mixpanel reserved/special profile properties (get $ prefix)
SPECIAL_PROPS = [
    "name", "first_name", "last_name", "email", "phone", "avatar", "created",
    "insert_id", "city", "region", "lib_version", "os", "os_version",
    "browser", "browser_version", "app_build_number", "app_version_string",
    "device", "screen_height", "screen_width", "screen_dpi", "current_url",
    "initial_referrer", "initial_referring_domain", "referrer",
    "referring_domain", "search_engine", "manufacturer", "brand", "model",
    "watch_model", "carrier", "radio", "wifi", "bluetooth_enabled",
    "bluetooth_version", "has_nfc", "has_telephone", "google_play_services",
    "duration", "country", "country_code",
]

# Properties that belong at root level (outside $set)
OUTSIDE_PROPS = ["distinct_id", "group_id", "token", "group_key", "ip"]

# Valid profile operations
VALID_OPERATIONS = ["$set", "$set_once", "$add", "$union", "$append", "$remove", "$unset"]

# Known bad user IDs that should be filtered
BAD_USER_IDS = [
    "-1", "0", "00000000-0000-0000-0000-000000000000", "<nil>", "[]",
    "anon", "anonymous", "false", "lmy47d", "n/a", "na", "nil", "none",
    "null", "true", "undefined", "unknown", "{}",
]

# Max string length for property values
MAX_STR_LEN = 255

# Time field aliases (checked in priority order)
TIME_FIELD_ALIASES = ["timestamp", "event_time", "ts_utc", "ts"]

# File extensions by format
JSONL_EXTENSIONS = {".jsonl", ".ndjson"}
JSON_EXTENSIONS = {".json"}
CSV_EXTENSIONS = {".csv", ".tsv"}
PARQUET_EXTENSIONS = {".parquet"}
GZIP_EXTENSIONS = {".gz", ".gzip"}

# Compression config
GZIP_LEVEL = 6
GZIP_CHUNK_SIZE = 16 * 1024  # 16 KB

# Default epoch bounds
DEFAULT_EPOCH_START = 0
DEFAULT_EPOCH_END = 9991427224
