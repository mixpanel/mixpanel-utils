# Mixpanel Project Structure

```mermaid
erDiagram
    PROJECT ||--o{ DASHBOARD : contains
    PROJECT ||--o{ COHORT : contains
    PROJECT ||--o{ BOOKMARK : contains
    PROJECT ||--o{ EVENT : contains
    PROJECT ||--o{ PEOPLE_PROFILE : contains

    DASHBOARD ||--o{ BOOKMARK : "contents.report{}"
    DASHBOARD }o--o{ COHORT : "filters[].filterValue[].cohort"
    DASHBOARD ||--|| LAYOUT : "has (read-only via API)"

    BOOKMARK }o--|| DASHBOARD : "dashboard_id (PATCH only)"
    BOOKMARK }o--o{ COHORT : "params JSON string"

    LAYOUT ||--o{ ROW : "rows{}"
    ROW ||--o{ CELL : "cells[]"
    CELL }o--|| BOOKMARK : "content_id"

    PROJECT {
        int id
        string token
        string api_secret
    }

    DASHBOARD {
        int id
        string title
        string description "must be null not empty string"
        json filters
        json layout "READ-ONLY via API"
        json contents
        bool can_update_basic
    }

    BOOKMARK {
        int id
        string name
        string type "insights funnels retention flows"
        string description "must be string not null"
        int dashboard_id "ignored on POST use PATCH"
        string params "JSON-encoded string"
    }

    COHORT {
        int id
        string name
        json groups "one of three required"
        json behaviors "one of three required"
        json selector "one of three required"
    }

    LAYOUT {
        json order "row ordering"
        json rows "keyed by row_id"
    }

    ROW {
        string row_id
        json cells
    }

    CELL {
        string content_type "report"
        int content_id "bookmark_id"
    }

    EVENT {
        string event
        json properties
        int time
    }

    PEOPLE_PROFILE {
        string distinct_id
        json properties
    }
```

## Authentication

```mermaid
flowchart LR
    subgraph "App API (HTTP Basic Auth)"
        SA[Service Account] --> D[Dashboards]
        SA --> B[Bookmarks/Reports]
        SA --> C[Cohorts]
    end

    subgraph "Export/Import API (mixpanel_utils)"
        API[API_SECRET + TOKEN] --> E[Events]
        API --> P[People Profiles]
    end
```

## API Endpoints

| Resource | Method | Endpoint | Notes |
|----------|--------|----------|-------|
| Cohorts | POST | `/api/app/projects/{id}/cohorts` | Requires `groups`, `behaviors`, or `selector` |
| Dashboards | GET | `/api/app/projects/{id}/dashboards/{did}` | Includes layout (read-only) |
| Dashboards | POST | `/api/app/projects/{id}/dashboards` | `description` must be `null` |
| Dashboards | PATCH | `/api/app/projects/{id}/dashboards/{did}` | Can update filters, NOT layout |
| Reports | GET | `/api/app/projects/{id}/bookmarks` | Lists all bookmarks |
| Reports | POST | `/api/app/projects/{id}/bookmarks` | `params` = JSON string; ignores `dashboard_id` |
| Reports | PATCH | `/api/app/projects/{id}/bookmarks/{bid}` | Use this to set `dashboard_id` |

## API Quirks

- **Dashboard layout is read-only** — cannot be written via API (returns 500/403). Use Mixpanel's "Move to project" UI feature to preserve layouts.
- **`POST /bookmarks` ignores `dashboard_id`** — must `PATCH` the bookmark afterward to link it to a dashboard.
- **Cohort IDs in report params are doubly-encoded** — JSON string within JSON, so cohort IDs appear as `\"id\": 123` (escaped).
- **Dashboard `description`** must be `null` (not empty string) or the API returns 400.
- **Bookmark `description`** must be a string (not `null`) or the API returns 400.
- All endpoints retry on `429` responses, respecting the `Retry-After` header.
