# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.0.0] - 2026-03-10

### BREAKING CHANGES

- **Removed API Secret (Project Secret) authentication support**
  - [Project Secret authentication](https://developer.mixpanel.com/reference/project-secret) has been deprecated and will be fully retired by Mixpanel on March 3, 2027
  - All users must migrate to [Service Account authentication](https://developer.mixpanel.com/reference/service-accounts)
  - The `api_secret` parameter has been renamed to `service_account_password`
  - The following parameters are now **REQUIRED**:
    - `service_account_username` - Your Service Account username
    - `service_account_password` - Your Service Account password/secret
    - `project_id` - Your Mixpanel project ID
  - Attempting to initialize without these required parameters will raise a `ValueError`

### Migration Guide

**Old initialization (no longer supported):**
```python
mputils = MixpanelUtils('project_api_secret', token='token')
```

**New initialization (required):**
```python
mputils = MixpanelUtils(
    service_account_username='my-user.12345.mp-service-account',
    service_account_password='service_account_password_here',
    project_id=project_id_here,
    token='project_token_here'
)
```

To create a Service Account, visit your Mixpanel project settings or refer to the [Service Accounts documentation](https://developer.mixpanel.com/reference/service-accounts).

### Changed

- Parameter order changed to prioritize Service Account credentials:
  1. `service_account_username` (required)
  2. `service_account_password` (required, formerly `api_secret`)
  3. `project_id` (required)
  4. `token` (optional, required for imports)
  5. Other optional parameters...
- Added a focused pytest regression suite for `MixpanelUtils.__init__` Service Account authentication requirements, including rejection of legacy API Secret-only usage.
- Corrected the README example script link to point to `tools/mixpanel_utils_example.py`.

### Removed

- Support for Project Secret (API Secret) authentication
- Conditional logic for Service Account vs API Secret authentication

## [2.2.7] - Previous Release

- Including group analytic operations
- Add support for India Residency
- Various bug fixes and improvements
