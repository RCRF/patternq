# patternq

A library for querying the Pattern.org data commons, and therefore interfacing with
the UnifyBio tool ecosystem.

## Installation

Install via pip git coordinates:

```
pip install git+ssh://git@github.com/RCRF/patternq.git
```

As this project is being rapidly updated, you should ensure you're using
the latest version by re-installing frequently via:

```
pip install --upgrade --force-reinstall git+ssh://git@github.com/RCRF/patternq.git
```

## Configuring access

Manage access to the query service by setting the endpoint and user credentials with environment variables:

```
PATTERNQ_ENDPOINT=https://data-commons.rcrf-dev.org
PATTERNQ_API_KEY=${USER_API_KEY}
```

To obtain a user API key, access user settings from the Unify Central dashboard
[here](https://data-commons.rcrf-dev.org/user-settings).

## Import renames & other conventions of use

The import alias naming convention 'pq' for the top level name, and `pq` + letter of
submodule, e.g. `pqd` for other cases is recommended for consistency across projects.
Examples:

```
import patternq as pq
import patternq.query as pqq
import patternq.datasets as pqd
import patternq.reference as pqr
```

## LICENSE

Made available under the Apache 2.0 License by the Rare Cancer Research Foundation.

