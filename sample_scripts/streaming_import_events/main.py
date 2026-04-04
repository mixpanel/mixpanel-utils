import asyncio
from mixpanel_utils import MixpanelUtils

# ----- Configuration
DATA_FILE = './events.jsonl'  # supports .jsonl, .json, .csv, .parquet, .json.gz
CREDENTIALS = {
    "project_id": None,  # REQUIRED - your Mixpanel project ID
    "service_account_username": "",  # REQUIRED - service account username
    "service_account_password": "",  # REQUIRED - service account password
    "token": "",  # REQUIRED - project token (for imports)
}
# ----- Configuration

mputils = MixpanelUtils(
    service_account_username=CREDENTIALS['service_account_username'],
    service_account_password=CREDENTIALS['service_account_password'],
    project_id=CREDENTIALS['project_id'],
    token=CREDENTIALS['token'],
)


async def main():
    # Basic import — format is auto-detected from file extension
    result = await mputils.stream.import_events(DATA_FILE)
    print_result("Basic import", result)

    # Import with transforms — fix data issues and deduplicate
    result = await mputils.stream.import_events(DATA_FILE, {
        'fix_data': True,
        'dedupe': True,
        'workers': 10,
    })
    print_result("Import with transforms", result)


def print_result(label, result):
    status = "PASS" if result.get('failed', 0) == 0 else "FAIL"
    print(f"\n[{status}] {label}")
    print(f"  total: {result.get('total', 0):,}  success: {result.get('success', 0):,}  failed: {result.get('failed', 0):,}")
    if result.get('duration_human'):
        print(f"  duration: {result['duration_human']}  requests: {result.get('requests', 0):,}")
    if result.get('errors'):
        for msg, count in result['errors'].items():
            print(f"  error: {msg} (x{count})")


asyncio.run(main())
