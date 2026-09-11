# Endpoint data-format audit

Source: [production Swagger](https://cwms-data.usace.army.mil/cwms-data/swagger-docs),
retrieved 2026-09-10, specification release `2026.05.12-i`.

This audit covers every existing endpoint wrapper, including paged/chunked
time-series requests, XML rating writes, and the direct session call in
`delete_user_roles`. It checks data-format selection, not full payload/schema
compatibility or successful authenticated CRUD against a database.

`api_version` is a historical data-format selector, not a service release:

- `1`: `application/json`, the server's unversioned default representation.
- `2`: `application/json;version=2`.
- `102`: `application/xml;version=2`.

GET uses the successful response content types in Swagger. POST/PATCH and the
body-bearing user-role DELETE use request-body content types. Error responses
are excluded. Operations with no request body or no successful response content
do not impose a data-version constraint; their existing selector is retained.
There are no existing PUT wrappers.

Special cases:

- BLOB retrieval streams the stored media type; its controller does not negotiate
  a versioned JSON representation from Accept. The existing retrieval behavior
  is retained. BLOB creation requires JSON v2, while BLOB PATCH also documents
  unversioned JSON.
- The spec lists explicit JSON v1 for individual projects, measurement extents,
  turbine changes, and pump accounting writes. These use the existing selector 1
  (unversioned JSON), which CDA resolves to its default representation. This is
  not an explicit v1 pin. The live measurement-extents response was HTTP 200 with
  `Content-Type: application/json;version=1` for unversioned Accept. The CDA
  DTO formatter annotations explicitly alias unversioned JSON to v1; see
  [Project](https://github.com/USACE/cwms-data-api/blob/9f1ffc59732da46d5543e2e9c737f05846fd4aa3/cwms-data-api/src/main/java/cwms/cda/data/dto/project/Project.java#L44),
  [TurbineChange](https://github.com/USACE/cwms-data-api/blob/9f1ffc59732da46d5543e2e9c737f05846fd4aa3/cwms-data-api/src/main/java/cwms/cda/data/dto/location/kind/TurbineChange.java#L41),
  and [WaterSupplyAccounting](https://github.com/USACE/cwms-data-api/blob/9f1ffc59732da46d5543e2e9c737f05846fd4aa3/cwms-data-api/src/main/java/cwms/cda/data/dto/watersupply/WaterSupplyAccounting.java#L42).
- Catalog dataset names and special-character BLOB/CLOB IDs resolve to the
  corresponding templated Swagger paths. Both `get_timeseries` branches and
  `get_timeseries_chunk` use v2. Both synchronous and threaded `store_timeseries`
  calls also use v2 through the same POST helper.

Validation:

- 28 header regression cases fail on unchanged main and pass with the fix.
- 34 prepared-request header checks cover every changed wrapper plus unchanged
  reads/writes on mixed-format resources and the prior project retrieval fix.
- The full mock/doctest suite passes: 132 tests.
- Live GET `/timeseries/profile?office-mask=SPK&location-mask=TEST` returns HTTP
  406 for `Accept: application/json;version=2` and HTTP 200 for
  `Accept: application/json`. No live write requests were made.

## Complete wrapper inventory

The format column is the selected wire media type, abbreviated as JSON, JSON v2,
or XML v2. A dash means Swagger defines no versioned payload for that operation.
"Default JSON" marks the explicit-v1 Swagger cases discussed above.

Swagger SHA-256: `964fc78c22b8af7721ef88f5cb23700c41f812f1e83047a7f17622161847bd03`.


### `cwms/catalog/blobs.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_blob` | GET | `blobs/{blob_id}` | Stored media type |
| `get_blobs` | GET | `blobs` | JSON v2 |
| `store_blobs` | POST | `blobs` | JSON v2 |
| `delete_blob` | DELETE | `blobs/{blob_id}` | — |
| `update_blob` | PATCH | `blobs/{blob_id}` | JSON |

### `cwms/catalog/catalog.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_locations_catalog` | GET | `catalog/LOCATIONS` | JSON v2 |
| `get_timeseries_catalog` | GET | `catalog/TIMESERIES` | JSON v2 |

### `cwms/catalog/clobs.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_clob` | GET | `clobs/{clob_id}` | JSON v2 |
| `get_clobs` | GET | `clobs` | JSON v2 |
| `delete_clob` | DELETE | `clobs/{clob_id}` | — |
| `update_clob` | PATCH | `clobs/{clob_id}` | JSON v2 |
| `store_clobs` | POST | `clobs` | JSON v2 |

### `cwms/forecast/forecast_instance.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_forecast_instances` | GET | `forecast-instance` | JSON v2 |
| `get_forecast_instance` | GET | `forecast-instance/{spec_id}` | JSON v2 |
| `store_forecast_instance` | POST | `forecast-instance` | JSON v2 |
| `delete_forecast_instance` | DELETE | `forecast-instance/{spec_id}` | — |

### `cwms/forecast/forecast_spec.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_forecast_specs` | GET | `forecast-spec` | JSON v2 |
| `get_forecast_spec` | GET | `forecast-spec/{spec_id}` | JSON v2 |
| `store_forecast_spec` | POST | `forecast-spec` | JSON v2 |
| `delete_forecast_spec` | DELETE | `forecast-spec/{spec_id}` | — |

### `cwms/levels/location_levels.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_location_levels` | GET | `levels` | JSON v2 |
| `get_location_level` | GET | `levels/{level_id}` | JSON v2 |
| `store_location_level` | POST | `levels` | JSON |
| `delete_location_level` | DELETE | `levels/{location_level_id}` | — |
| `update_location_level` | PATCH | `levels/{level_id}` | JSON |
| `get_level_as_timeseries` | GET | `levels/{location_level_id}/timeseries` | JSON v2 |

### `cwms/levels/specified_levels.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_specified_levels` | GET | `specified-levels` | JSON v2 |
| `store_specified_level` | POST | `specified-levels` | JSON v2 |
| `delete_specified_level` | DELETE | `specified-levels/{specified_level_id}` | — |
| `update_specified_level` | PATCH | `specified-levels/{old_specified_level_id}` | — |

### `cwms/locations/gate_changes.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_all_gate_changes` | GET | `projects/{office_id}/{project_id}/gate-changes` | JSON |
| `store_gate_change` | POST | `projects/gate-changes` | JSON |
| `delete_gate_change` | DELETE | `projects/{office_id}/{project_id}/gate-changes` | — |

### `cwms/locations/location_groups.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_location_group` | GET | `location/group/{loc_group_id}` | JSON |
| `get_location_groups` | GET | `location/group` | JSON |
| `store_location_groups` | POST | `location/group` | JSON |
| `update_location_group` | PATCH | `location/group/{group_id}` | JSON |
| `delete_location_group` | DELETE | `location/group/{group_id}` | — |

### `cwms/locations/lookups.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_all_lookups` | GET | `lookup-types` | JSON |
| `create_lookup` | POST | `lookup-types` | JSON |
| `update_lookup` | PATCH | `lookup-types/{category}` | JSON |
| `delete_lookup` | DELETE | `lookup-types/{display_value}` | — |

### `cwms/locations/physical_locations.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_location` | GET | `locations/{location_id}` | JSON v2 |
| `get_locations` | GET | `locations` | JSON v2 |
| `delete_location` | DELETE | `locations/{location_id}` | — |
| `store_location` | POST | `locations` | JSON |
| `update_location` | PATCH | `locations/{location_id}` | JSON |

### `cwms/measurements/measurements.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_measurements` | GET | `measurements` | JSON |
| `store_measurements` | POST | `measurements` | JSON |
| `delete_measurements` | DELETE | `measurements/{location_id}` | — |
| `get_measurements_extents` | GET | `measurements/time-extents` | Default JSON |

### `cwms/outlets/outlets.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_outlet` | GET | `projects/outlets/{name}` | JSON |
| `get_outlets` | GET | `projects/outlets` | JSON |
| `delete_outlet` | DELETE | `projects/outlets/{name}` | — |
| `rename_outlet` | PATCH | `projects/outlets/{old_name}` | — |
| `store_outlet` | POST | `projects/outlets` | JSON |

### `cwms/outlets/virtual_outlets.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_virtual_outlet` | GET | `projects/{office_id}/{project_id}/virtual-outlets/{name}` | JSON |
| `get_virtual_outlets` | GET | `projects/{office_id}/{project_id}/virtual-outlets` | JSON |
| `delete_virtual_outlet` | DELETE | `projects/{office_id}/{project_id}/virtual-outlets/{name}` | — |
| `store_virtual_outlet` | POST | `projects/virtual-outlets` | JSON |

### `cwms/projects/project_lock_rights.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_project_lock_rights` | GET | `project-lock-rights` | JSON |
| `remove_all_project_lock_rights` | POST | `project-lock-rights/remove-all` | — |
| `update_project_lock_rights` | POST | `project-lock-rights/update` | — |

### `cwms/projects/project_locks.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_project_lock` | GET | `project-locks/{name}` | JSON |
| `get_project_locks` | GET | `project-locks` | JSON |
| `revoke_project_lock` | DELETE | `project-locks/{name}` | — |
| `request_project_lock` | POST | `project-locks` | JSON |
| `deny_project_lock_request` | POST | `project-locks/deny` | — |
| `release_project_lock` | POST | `project-locks/release` | — |

### `cwms/projects/projects.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_project` | GET | `projects/{name}` | Default JSON |
| `get_projects` | GET | `projects` | JSON |
| `get_project_locations` | GET | `projects/locations` | JSON |
| `delete_project` | DELETE | `projects/{name}` | — |
| `rename_project` | PATCH | `projects/{old_name}` | JSON |
| `store_project` | POST | `projects` | JSON |
| `status_update` | POST | `projects/status-update/{project_id}` | — |

### `cwms/projects/water_supply/accounting.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_pump_accounting` | GET | `projects/{office_id}/{project_id}/water-user/{water_user}/contracts/{contract_name}/accounting` | JSON |
| `store_pump_accounting` | POST | `projects/{office}/{project_id}/water-user/{water_user}/contracts/{contract_name}/accounting` | Default JSON |

### `cwms/properties/properties.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_properties` | GET | `properties` | JSON |
| `get_property` | GET | `properties/{name}` | JSON |
| `create_property` | POST | `properties` | JSON |
| `update_property` | PATCH | `properties/{name}` | JSON |
| `delete_property` | DELETE | `properties/{name}` | — |

### `cwms/ratings/ratings.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_ratings_xml` | GET | `ratings/{rating_id}` | XML v2 |
| `get_ratings` | GET | `ratings/{rating_id}` | JSON v2 |
| `update_ratings` | PATCH | `ratings/{rating_id}` | JSON v2 |
| `delete_ratings` | DELETE | `ratings/{rating_id}` | — |
| `store_rating` | POST | `ratings` | JSON v2 |
| `_perform_value_rating` | POST | `ratings/rate-values/{office_id}/{rating_id}` | JSON |

### `cwms/ratings/ratings_spec.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_rating_spec` | GET | `ratings/spec/{rating_id}` | JSON v2 |
| `get_rating_specs` | GET | `ratings/spec` | JSON v2 |
| `delete_rating_spec` | DELETE | `ratings/spec/{rating_id}` | — |
| `store_rating_spec` | POST | `ratings/spec/` | XML v2 |

### `cwms/ratings/ratings_template.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_rating_template` | GET | `ratings/template/{template_id}` | JSON v2 |
| `get_rating_templates` | GET | `ratings/template` | JSON v2 |
| `delete_rating_template` | DELETE | `ratings/template/{template_id}` | — |
| `store_rating_template` | POST | `ratings/template/` | XML v2 |

### `cwms/standard_text/standard_text.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_standard_text_catalog` | GET | `standard-text-id` | JSON v2 |
| `get_standard_text` | GET | `standard-text-id/{text_id}` | JSON v2 |
| `delete_standard_text` | DELETE | `standard-text-id/{text_id}` | — |
| `store_standard_text` | POST | `standard-text-id` | JSON v2 |

### `cwms/timeseries/timeseries.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_timeseries_chunk` | GET | `timeseries` | JSON v2 |
| `get_timeseries` | GET | `timeseries` | JSON v2 |
| `store_timeseries` | POST | `timeseries` | JSON v2 |
| `delete_timeseries` | DELETE | `timeseries/{ts_id}` | — |

### `cwms/timeseries/timeseries_bin.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_binary_timeseries` | GET | `timeseries/binary` | JSON v2 |
| `store_binary_timeseries` | POST | `timeseries/binary` | JSON v2 |
| `delete_binary_timeseries` | DELETE | `timeseries/binary/{timeseries_id}` | — |

### `cwms/timeseries/timeseries_group.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_timeseries_group` | GET | `timeseries/group/{group_id}` | JSON |
| `get_timeseries_groups` | GET | `timeseries/group` | JSON |
| `store_timeseries_groups` | POST | `timeseries/group` | JSON |
| `update_timeseries_groups` | PATCH | `timeseries/group/{group_id}` | JSON |
| `delete_timeseries_group` | DELETE | `timeseries/group/{group_id}` | — |

### `cwms/timeseries/timeseries_identifier.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_timeseries_identifier` | GET | `timeseries/identifier-descriptor/{ts_id}` | JSON v2 |
| `get_timeseries_identifiers` | GET | `timeseries/identifier-descriptor/` | JSON v2 |
| `delete_timeseries_identifier` | DELETE | `timeseries/identifier-descriptor/{ts_id}` | — |
| `store_timeseries_identifier` | POST | `timeseries/identifier-descriptor/` | JSON v2 |

### `cwms/timeseries/timeseries_profile.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_timeseries_profile` | GET | `timeseries/profile/{location_id}/{parameter_id}` | JSON |
| `get_timeseries_profiles` | GET | `timeseries/profile` | JSON |
| `delete_timeseries_profile` | DELETE | `timeseries/profile/{location_id}/{parameter_id}` | — |
| `store_timeseries_profile` | POST | `timeseries/profile` | JSON |

### `cwms/timeseries/timeseries_profile_instance.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_timeseries_profile_instance` | GET | `timeseries/profile-instance/{location_id}/{parameter_id}/{version}` | JSON |
| `get_timeseries_profile_instances` | GET | `timeseries/profile-instance` | JSON |
| `delete_timeseries_profile_instance` | DELETE | `timeseries/profile-instance/{location_id}/{parameter_id}/{version}` | — |
| `store_timeseries_profile_instance` | POST | `timeseries/profile-instance` | JSON |

### `cwms/timeseries/timeseries_profile_parser.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_timeseries_profile_parser` | GET | `timeseries/profile-parser/{location_id}/{parameter_id}` | JSON |
| `get_timeseries_profile_parsers` | GET | `timeseries/profile-parser` | JSON |
| `delete_timeseries_profile_parser` | DELETE | `timeseries/profile-parser/{location_id}/{parameter_id}` | — |
| `store_timeseries_profile_parser` | POST | `timeseries/profile-parser` | JSON |

### `cwms/timeseries/timeseries_txt.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_text_timeseries` | GET | `timeseries/text` | JSON v2 |
| `store_text_timeseries` | POST | `timeseries/text` | JSON v2 |
| `delete_text_timeseries` | DELETE | `timeseries/text/{timeseries_id}` | — |

### `cwms/turbines/turbines.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_project_turbines` | GET | `projects/turbines` | JSON |
| `get_project_turbine` | GET | `projects/turbines/{name}` | JSON |
| `get_project_turbine_changes` | GET | `projects/{office}/{name}/turbine-changes` | Default JSON |
| `store_project_turbine` | POST | `projects/turbines` | JSON |
| `store_project_turbine_changes` | POST | `projects/{office}/{name}/turbine-changes` | JSON |
| `delete_project_turbine` | DELETE | `projects/turbines/{name}` | — |
| `delete_project_turbine_changes` | DELETE | `projects/{office}/{name}/turbine-changes` | — |

### `cwms/users/users.py`

| Function | HTTP | Path | Format |
| --- | --- | --- | --- |
| `get_roles` | GET | `roles` | JSON |
| `get_user_profile` | GET | `user/profile` | JSON |
| `get_users` | GET | `users` | JSON |
| `get_user` | GET | `users/{user_name}` | JSON |
| `store_user` | POST | `user/{user_name}/roles/{office_id}` | JSON |
| `update_user` | POST | `user/{user_name}/roles/{office_id}` | JSON |
| `delete_user_roles` | DELETE | `user/{user_name}/roles/{office_id}` | JSON |
