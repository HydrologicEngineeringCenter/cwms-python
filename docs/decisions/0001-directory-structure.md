# Directory Structure

## Summary

We want to formalize the file and directory structure for the `cwms-python` module source code.

Ideally, we would have a one-to-one mapping between CDA endpoints and the `cwms-python` modules that implement the interfaces for those endpoints. There are two primary benefits that we hope achieve:

1. `cwms-python` developers will be able to easily reference the CDA documentation related to the module they are working on.
2. CDA developers who want to look at the python implementation for a collection of endpoints will know exactly where to look, even if they are not a `cwms-python` developer.

The goal is to make it easier to update and maintain the project in the long term, and to reduce the effort required by new developers (and external collaborators) to contribute to the project. This decision is focused only on developer workflow and should have no impact on `cwms-python` users.

## Considered Options

Each top-level CWMS Data API endpoint corresponds to a `cwms-python` submodule. Nested endpoints would then map to a sub-directory within the submodule. The table below lists some example API endpoints and the corresponding submodule.

| API Endpoint                                  | CWMS Python Module           |
|-----------------------------------------------|------------------------------|
| `/cwms-data/location/category`                | `cwms.location.category`     |
| `/cwms-data/location/group`                   | `cwms.location.group`        |
| `/cwms-data/locations`                        | `cwms.locations`             |
| `/cwms-data/levels`                           | `cwms.levels`                |
| `/cwms-data/specified-levels`                 | `cwms.specified_levels`      |
| `/cwms-data/timeseries`                       | `cwms.timeseries`            |
| `/cwms-data/timeseries/category`              | `cwms.timeseries.category`   |
| `/cwms-data/timeseries/identifier-descriptor` | `cwms.timeseries.identifier` |
| `/cwms-data/timeseries/group`                 | `cwms.timeseries.group`      |

### Special Cases

The direct mapping outlined above works well for most of the API endpoints. However, there are a couple cases which may warrant special consideration.

#### Location Related Endpoints

The locations, location categories, and location groups API endpoints return related data, despite having different URL paths (`/cwms-data/locations` vs. `/cwms-data/location/{category,group})`). From a `cwms-python` perspective, it may make more sense (and be less confusing to the user) if the corresponding functions were defined in the same submodule. This could be either `cwms.location` or `cwms.locations`, but since we are using the plural case for other endpoints (`cwms.levels`, `cwms-timeseries`, etc), `cwms.locations` would be the more natural choice.

#### Levels and Specified Levels

This case is similar to the one above. The `/cwms-data/specified-level` endpoint returns data that is related to levels (returned from `/cwms-data/levels`). Again, it makes sense to organize these function in a single submodule. There are two possible approaches:

1. Define the specified levels functions in the `cwms.levels` submodule. This is how the code is currently organized.
2. Create a sub-directory in the levels submodule, similar to the locations and timeseries submodules. The specified levels functions would then be accessed from `cwms.levels.specified_levels`.

The first option is nice from user perspective, as it only requires a single import statement. The second option may be preferred for code maintainability, as it clearly separates the functionality of each CWMS Data API group (only a single `GET`, `POST`, etc call per submodule). A balanced approach would be to separate the code into different submodules, but import the functions in a top-level `__init__.py` file. This could be applied to all submodules with nested directories (locations, levels, timeseries. etc). In this scenario the directory and file structure for the location and level submodules would look like the following.

```
cwms
|--- levels
|    |--- __init__.py
|    |--- location_levels.py
|    |--- specified_levels.py
|--- locations
     |--- __init__.py
     |--- category.py
     |--- group.py
     |--- physical_locations.py
```

**NOTE:** The modules for the top-level `/cwms-data/levels` and `/cwms-data/locations` endpoints have been named `specified_levels.py` and `physical_locations.py` since this terminology is commonly used in CWMS and matches the underlying data schema.

### Example Usage

Users would have the option of importing directly from a specific submodule file (`cwms.levels.specified_levels`) if needed, but would generally import from the submodule itself, which would expose the functions defined in the nested files.

```python
# Import the entire submodule
import cwms.levels

# Import specific functions from top-level module (normal, documented usage)
from cwms.levels import get_location_levels, get_specified_levels

# Import directly from submodule (possible, but not recommend)
from cwms.levels.specified_level import get_specified_levels
```

## Decision Outcome

We will adopt the directory/file structure outlined above. Whenever possible we will directly map `cwms-python` module names to the corresponding CDA endpoints. Exceptions may be made, as in the cases discussed above, but these should be kept to minimum. This decision should be revisited as additional endpoints are added to the CDA.

## References

1. https://cwms-data-test.cwbi.us/cwms-data/swagger-ui.html