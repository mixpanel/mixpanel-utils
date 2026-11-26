import asyncio
from mixpanel_utils import MixpanelUtils

# ----- Configuration
DATA_FILE = './amplitude_export.json'  # path to vendor export file
VENDOR = 'amplitude'  # one of: amplitude, heap, ga4, posthog, mparticle, june, mixpanel
VENDOR_OPTIONS = {}  # optional vendor-specific config (passed as dict)
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
    print(f"Importing {VENDOR} data from {DATA_FILE}...")
    result = await mputils.stream.import_events(DATA_FILE, {
        'vendor': VENDOR,
        'vendor_opts': VENDOR_OPTIONS,
        'fix_data': True,
    })

    status = "PASS" if result.get('failed', 0) == 0 else "FAIL"
    print(f"\n[{status}] {VENDOR.title()} vendor migration")
    print(f"  total: {result.get('total', 0):,}  success: {result.get('success', 0):,}  failed: {result.get('failed', 0):,}")
    if result.get('duration_human'):
        print(f"  duration: {result['duration_human']}  requests: {result.get('requests', 0):,}")
    if result.get('errors'):
        for msg, count in result['errors'].items():
            print(f"  error: {msg} (x{count})")


asyncio.run(main())
