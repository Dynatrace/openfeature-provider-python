# Dynatrace OpenFeature Provider for Python

The official [Dynatrace](https://dynatrace.com) [OpenFeature](https://openfeature.dev) provider for Python server-side applications.

> **Status:** Early alpha — not yet ready for production use!

## Installation

```bash
pip install dynatrace-openfeature-provider-python
```

## Usage

```python
from openfeature import api
from openfeature.evaluation_context import EvaluationContext
from dynatrace.openfeature import DynatraceProvider, DynatraceProviderOptions

options = DynatraceProviderOptions.from_sdk_key(
    'dt01.server_us_<key>.de848e97..._a1b2c3d4e5'
)

api.set_provider(DynatraceProvider(options))
client = api.get_client()

ctx = EvaluationContext(targeting_key='user-123')
enabled = client.get_boolean_value('my-feature', False, ctx)

# On shutdown:
api.shutdown()
```

### Config origin override (labs / dev environments)

By default the config endpoint is derived from the SDK key. To point the provider at a
different environment or at an SDK proxy, pass `config_origin`. The value is an origin
only — scheme, host, and optional port, with no path. The provider appends the platform
and key segments itself.

```python
api.set_provider(
    DynatraceProvider(sdk_key, config_origin='https://cdn.eu.fm.dynatracelabs.com')
)
```

## Architecture

The provider performs in-process flag evaluation using [flagd-core](https://github.com/open-feature/python-sdk-contrib/tree/main/tools/openfeature-flagd-core).
On initialization it fetches a flagd-format JSON config from the Dynatrace CDN and polls for
updates every 10 seconds. No external bucketing API is called during evaluation.

State transitions follow [providers.md §4.1](https://github.com/Dynatrace/feature-management-knowledge-base):
`NOT_READY → READY → STALE → ERROR`, with a 1-minute grace period before STALE → ERROR.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache 2.0 — see [LICENSE](LICENSE).
