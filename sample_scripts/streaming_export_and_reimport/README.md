# Cross-project migration (export & reimport)

This script demonstrates how to migrate data between Mixpanel projects using `mputils.stream.export_import_events`. It exports events from one project and re-imports them into another in a single operation — no intermediate files needed.

Common use cases:

- **Project consolidation** — Merge events from multiple projects into one
- **Environment promotion** — Copy production data to a staging project
- **Region migration** — Move data between US, EU, and IN residency regions

The same pattern works for user profiles (`export_import_people`) and group profiles (`export_import_groups`).

## Configuration

Edit `main.py` with your source project credentials and the target project's token. Set the date range for the events you want to migrate.

## Installation

```
pip3 install mixpanel-utils[streaming]
```
