from dynatrace.openfeature import DynatraceProvider


def test_provider_importable() -> None:
    provider = DynatraceProvider(
        "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
    )
    assert provider.get_metadata().name == "dynatrace"
