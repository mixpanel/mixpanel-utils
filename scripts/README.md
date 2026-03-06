# Mixpanel Project Migration Scripts

Scripts for migrating dashboards, reports, and cohorts between Mixpanel projects.

## Prerequisites

- Python 3.8+
- `requests` library (`pip install requests`)
- `mixpanel-utils` library (`pip install mixpanel-utils`) — used by event/people export/import scripts
- A `.env` file in the project root with the following variables:

```env
# Source project
FROM_PROJECT_ID=...
FROM_PROJECT_API_SECRET=...           # Used by export scripts (mixpanel_utils)
FROM_PROJECT_TOKEN=...
FROM_SERVICE_ACCOUNT_USERNAME=...     # Used by App API scripts (dashboards, cohorts, reports)
FROM_SERVICE_ACCOUNT_PASSWORD=...

# Target project
TO_PROJECT_ID=...
TO_PROJECT_API_SECRET=...             # Used by import scripts (mixpanel_utils)
TO_PROJECT_TOKEN=...                  # Required for event/people imports
TO_SERVICE_ACCOUNT_USERNAME=...       # Used by App API scripts
TO_SERVICE_ACCOUNT_PASSWORD=...
```

Service accounts need **Admin** role on their respective projects.

## Migration Workflow

There are two approaches to migrating dashboards. **Approach A** (recommended) preserves dashboard layouts. **Approach B** creates reports via API but dashboards end up without visual layout.

### Approach A: Move Dashboards via UI + Remap (Recommended)

This approach uses Mixpanel's built-in "Move to project" feature to preserve dashboard layouts, then fixes cohort references via API.

```
1. Import cohorts           -> produces cohort_mapping.json
2. Move dashboards via UI   -> preserves layout, but cohort IDs are stale
3. Remap cohort IDs         -> fixes cohort references on moved dashboards
```

**Step 1: Import cohorts to the target project**

```bash
python scripts/create_cohorts_from_json.py
```

Reads `data/cohorts.json`, creates cohorts in the target project, and saves `cohort_mapping.json` (old ID -> new ID).

**Step 2: Move dashboards via Mixpanel UI**

In the source project, open each dashboard -> **...** menu -> **Move to project** -> select the target project.

> **Important:** After moving, share each dashboard with the service account as **Editor** (Dashboard -> Share -> "Give access to everyone in the project" -> Editor).

**Step 3: Remap cohort IDs on moved dashboards**

To remap all dashboards listed in `data/from_ui_dashboard_mapping.json`:

```bash
python scripts/remap_all_moved_dashboards.py
```

Or to remap individual dashboards by ID:

```bash
python scripts/remap_moved_dashboard.py <dashboard_id> [<dashboard_id> ...]
```

Updates dashboard filters and all report params to use the new cohort IDs.

The `from_ui_dashboard_mapping.json` file maps source dashboard IDs to target dashboard IDs (fill this in after moving dashboards via the UI).

---

### Approach B: Full API Import (No Layout)

This approach creates everything via API. Reports are created correctly but **dashboard layouts will be empty** (Mixpanel's API does not support writing layouts). Use this only if you don't need visual layout preservation.

```
1. Import cohorts               -> cohort_mapping.json
2. Remap cohorts in dashboards  -> dashboards_remapped.json
3. Import dashboards            -> dashboard_mapping.json
4. Remap cohorts in reports     -> reports_remapped.json
5. Remap dashboards in reports  -> reports_ready.json
6. Import reports (bookmarks)
7. Link reports to dashboards
```

**Step 1: Import cohorts**

```bash
python scripts/create_cohorts_from_json.py
```

**Step 2: Remap cohort IDs in dashboard definitions**

```bash
python scripts/remap_dashboard_cohorts.py
```

Reads `data/dashboards.json` + `data/cohort_mapping.json`, writes `data/dashboards_remapped.json`.

**Step 3: Import dashboards**

```bash
python scripts/import_dashboards.py
```

Reads `data/dashboards_remapped.json`, creates dashboards, saves `data/dashboard_mapping.json`.

**Step 4: Remap cohort IDs in report definitions**

```bash
python scripts/remap_report_cohorts.py
```

Reads `data/reports.json` + `data/cohort_mapping.json`, writes `data/reports_remapped.json`. Handles cohort IDs inside JSON-encoded strings (report params).

**Step 5: Remap dashboard IDs in reports**

```bash
python scripts/remap_report_dashboards.py
```

Reads `data/reports_remapped.json` + `data/dashboard_mapping.json`, writes `data/reports_ready.json`.

**Step 6: Import reports**

```bash
python scripts/import_reports.py
```

Creates bookmarks (reports) in the target project via `POST /api/app/projects/{id}/bookmarks`.

**Step 7: Link reports to dashboards**

```bash
python scripts/link_reports_to_dashboards.py
```

PATCHes each bookmark to set its `dashboard_id`, matching by name + type.

> **Note:** After this, reports will exist in the dashboard's `contents.report` but the dashboard layout (visual arrangement) will be empty. Mixpanel's API does not support writing dashboard layouts.

---

## Event & People Migration

These scripts use the `mixpanel_utils` library (not the App API) and require `FROM_PROJECT_API_SECRET` for exports, `TO_PROJECT_API_SECRET` + `TO_PROJECT_TOKEN` for imports.

**Export events from source project:**

```bash
python scripts/export_events_from_project.py
```

> **Note:** Edit `FROM_DATE` and `TO_DATE` in the script before running. Events are exported one month at a time to avoid timeouts. The `EXCLUDE_EVENTS` list can be modified to skip specific events (defaults to excluding `session_completed`). Output goes to `events/events_export_from_{FROM_PROJECT_ID}_{date_range}.json`.

**Import events to target project:**

```bash
python scripts/import_events_to_project.py                    # imports all files from events/
python scripts/import_events_to_project.py events/file.json   # imports a single file
```

> Uses `TO_PROJECT_API_SECRET` and `TO_PROJECT_TOKEN`. The `TIMEZONE_OFFSET` constant (default `0` = UTC) should match the source project's timezone offset.

**Export people profiles:**

```bash
python scripts/export_people_from_project.py
```

> Uses `FROM_PROJECT_API_SECRET`. Output goes to `people_export_from_{FROM_PROJECT_ID}.json` in the project root.

**Import people profiles:**

```bash
python scripts/import_people_to_project.py                              # imports default file
python scripts/import_people_to_project.py people_export_from_123.json  # imports a specific file
```

> Uses `TO_PROJECT_API_SECRET` and `TO_PROJECT_TOKEN`.

## Data Files (`data/`)

| File | Description |
|------|-------------|
| `cohorts.json` | Exported cohort definitions from source project |
| `cohort_mapping.json` | Old cohort ID -> new cohort ID |
| `dashboards.json` | Exported dashboard definitions from source project |
| `dashboards_remapped.json` | Dashboards with cohort IDs remapped |
| `dashboard_mapping.json` | Old dashboard ID -> new dashboard ID |
| `reports.json` | Exported report definitions from source project |
| `reports_remapped.json` | Reports with cohort IDs remapped |
| `reports_ready.json` | Reports with both cohort and dashboard IDs remapped |
| `from_ui_dashboard_mapping.json` | Source dashboard ID -> target dashboard ID (after UI move) |
| `bookmark_mapping.json` | Old bookmark/report ID -> new bookmark/report ID |

## API Notes

- **Cohorts**: `POST /api/app/projects/{id}/cohorts` — requires `groups`, `behaviors`, or `selector` fields
- **Dashboards**: `POST /api/app/projects/{id}/dashboards` — `description` must be `null` (not empty string)
- **Reports**: `POST /api/app/projects/{id}/bookmarks` — `params` must be a JSON string, `description` must be a string (not `null`)
- **Linking reports**: `PATCH /api/app/projects/{id}/bookmarks/{bid}` with `{"dashboard_id": ...}`
- **Dashboard layouts**: Read-only via API. Use Mixpanel's "Move to project" UI feature to preserve layouts.
- **Auth**: All App API endpoints use HTTP Basic Auth with service account credentials.
- **Rate limits**: All scripts retry on 429 responses, respecting the `Retry-After` header.
