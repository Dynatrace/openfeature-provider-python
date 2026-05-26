from dynatrace.openfeature import DynatraceProvider


def test_provider_importable() -> None:
    provider = DynatraceProvider()
    assert provider.get_metadata().name == "dynatrace"
