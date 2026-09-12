import asyncio
from mixpanel_utils import MixpanelUtils

# ----- Configuration
SOURCE_CREDENTIALS = {
    "project_id": None,  # REQUIRED - source Mixpanel project ID
    "service_account_username": "",  # REQUIRED - service account with access to source project
    "service_account_password": "",  # REQUIRED - service account password
}
TARGET_TOKEN = ""  # REQUIRED - project token of the destination project
DATE_RANGE = {
    "start": "2024-01-01",  # export start date (YYYY-MM-DD)
    "end": "2024-12-31",  # export end date (YYYY-MM-DD)
}
# ----- Configuration

mputils = MixpanelUtils(
    service_account_username=SOURCE_CREDENTIALS['service_account_username'],
    service_account_password=SOURCE_CREDENTIALS['service_account_password'],
    project_id=SOURCE_CREDENTIALS['project_id'],
)


async def main():
    # Export events from source and import into target project
    print(f"Migrating events from {DATE_RANGE['start']} to {DATE_RANGE['end']}...")
    result = await mputils.stream.export_import_events({
        'start': DATE_RANGE['start'],
        'end': DATE_RANGE['end'],
        'second_token': TARGET_TOKEN,
    })

    status = "PASS" if result.get('failed', 0) == 0 else "FAIL"
    print(f"\n[{status}] Cross-project event migration")
    print(f"  total: {result.get('total', 0):,}  success: {result.get('success', 0):,}  failed: {result.get('failed', 0):,}")
    if result.get('duration_human'):
        print(f"  duration: {result['duration_human']}")
    if result.get('errors'):
        for msg, count in result['errors'].items():
            print(f"  error: {msg} (x{count})")


asyncio.run(main())
