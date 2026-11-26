# Migrating from another analytics vendor

This script demonstrates how to import event data exported from another analytics platform into Mixpanel using the `vendor` option. The streaming interface automatically maps vendor-specific fields (event names, property names, timestamps, user IDs) to Mixpanel's format.

Supported vendors:

- **Amplitude** — Amplitude event exports
- **Heap** — Heap event data
- **GA4** — Google Analytics 4 BigQuery exports
- **PostHog** — PostHog event data
- **mParticle** — mParticle event data
- **June** — June analytics data
- **Mixpanel** — Mixpanel's own export format (useful for re-importing exported data)

## Configuration

Edit `main.py` with your Mixpanel credentials, the path to your vendor export file, and the vendor name. Optionally configure `VENDOR_OPTIONS` for vendor-specific settings.

## Installation

```
pip3 install mixpanel-utils[streaming]
```
