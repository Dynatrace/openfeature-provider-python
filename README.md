# Dynatrace OpenFeature Provider for Python

The official [Dynatrace](https://dynatrace.com) [OpenFeature](https://openfeature.dev) provider for Python server-side applications.

The provider evaluates flags in-process: on initialization it fetches your flag configuration from the Dynatrace CDN and refreshes it in the background, so evaluations are local and do not make a network call.

Changes are picked up through a server-sent events (SSE) stream advertised in the flag configuration: a change notification triggers an immediate re-fetch, and polling every 10 minutes acts as a safety net. While the stream is unavailable or disabled, the provider polls every 10 seconds instead.

> **Status:** Early alpha — not yet ready for production use.

## Installation

```bash
pip install dynatrace-openfeature-provider-python
```

## Usage

```python
from openfeature import api
from openfeature.evaluation_context import EvaluationContext

from dynatrace.openfeature import DynatraceProvider

api.set_provider(DynatraceProvider("dt01.server_us_<key>"))

client = api.get_client()
ctx = EvaluationContext(targeting_key="user-123")
enabled = client.get_boolean_value("my-flag", False, ctx)

# On shutdown:
api.shutdown()
```

Once the provider is registered, all flag evaluation goes through the OpenFeature API. See the
[OpenFeature Python SDK documentation](https://openfeature.dev/docs/reference/technologies/server/python/)
for evaluation contexts, hooks, events, and the rest of the SDK surface.

## Configuration

Pass options as keyword arguments to `DynatraceProvider`:

```python
api.set_provider(
    DynatraceProvider(
        sdk_key,
        config_origin="https://cdn.eu.fm.dynatracelabs.com",
        metrics_origin="https://metrics.eu.fm.dynatracelabs.com",
    )
)
```

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `config_origin` | `str` | derived from SDK key | Override the flag configuration origin. |
| `metrics_origin` | `str` | derived from SDK key | Override the evaluation metrics ingest origin. |
| `disable_sse` | `bool` | `False` | Don't attempt an SSE connection and poll only, every 10 seconds. |

## Running the e2e tests

The shared [provider test harness](https://github.com/Dynatrace/fm-provider-test-harness) gherkin suite runs against its mock server in a container, so Docker is required:

```bash
uv run poe e2e
```

This initializes the `fm-provider-test-harness` submodule and runs the suite with pytest-bdd. Set `SKIP_HARNESS_UPDATE=1` to keep a local harness checkout, and `MOCKSERVER_IMAGE` to use a locally built mock server image.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache 2.0 — see [LICENSE](LICENSE).
