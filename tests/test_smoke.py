from dynatrace.openfeature import DynatraceProvider, DynatraceProviderOptions


def test_provider_importable() -> None:
    opts = DynatraceProviderOptions.from_sdk_key(
        'dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5'
    )
    provider = DynatraceProvider(opts)
    assert provider.get_metadata().name == 'dynatrace'
