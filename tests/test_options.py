import pytest

from dynatrace.openfeature.options import DynatraceProviderOptions

_VALID_KEY = "dt01.server_us_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"


class TestFromSdkKey:
    def test_valid_key_parses(self) -> None:
        opts = DynatraceProviderOptions.from_sdk_key(_VALID_KEY)
        assert opts.cdn_url == (
            "https://cdn.us.fm.dynatrace.com/server/" + _VALID_KEY + ".json"
        )

    def test_public_key_excludes_secret(self) -> None:
        opts = DynatraceProviderOptions.from_sdk_key(_VALID_KEY)
        assert opts.public_key == "dt01.server_us_abcdef1234"
        assert "de848e97" not in opts.public_key

    def test_whitespace_trimmed(self) -> None:
        opts = DynatraceProviderOptions.from_sdk_key("  " + _VALID_KEY + "  ")
        assert opts.public_key == "dt01.server_us_abcdef1234"

    def test_blank_key_raises(self) -> None:
        with pytest.raises(ValueError, match="blank"):
            DynatraceProviderOptions.from_sdk_key("   ")

    def test_empty_key_raises(self) -> None:
        with pytest.raises(ValueError, match="blank"):
            DynatraceProviderOptions.from_sdk_key("")

    def test_invalid_format_raises(self) -> None:
        with pytest.raises(ValueError, match="invalid key format"):
            DynatraceProviderOptions.from_sdk_key("not-a-valid-key")

    def test_non_server_platform_raises(self) -> None:
        # The regex only matches 'server' — a 'client' key fails regex validation
        client_key = _VALID_KEY.replace("server_", "client_", 1)
        with pytest.raises(ValueError, match="invalid key format"):
            DynatraceProviderOptions.from_sdk_key(client_key)

    def test_geo_reflected_in_cdn_url(self) -> None:
        eu_key = "dt01.server_eu_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
        opts = DynatraceProviderOptions.from_sdk_key(eu_key)
        assert "cdn.eu.fm.dynatrace.com" in opts.cdn_url

    def test_config_origin_override(self) -> None:
        opts = DynatraceProviderOptions.from_sdk_key(
            _VALID_KEY, config_origin="https://cdn.us.fm.dynatracelabs.com"
        )
        assert opts.cdn_url.startswith("https://cdn.us.fm.dynatracelabs.com/server/")

    def test_config_origin_keeps_default_port(self) -> None:
        opts = DynatraceProviderOptions.from_sdk_key(
            _VALID_KEY, config_origin="https://cdn.example.com:443"
        )
        assert opts.cdn_url.startswith("https://cdn.example.com:443/server/")

    def test_config_origin_trailing_slash_normalised(self) -> None:
        opts_slash = DynatraceProviderOptions.from_sdk_key(
            _VALID_KEY, config_origin="https://example.com/"
        )
        opts_no_slash = DynatraceProviderOptions.from_sdk_key(
            _VALID_KEY, config_origin="https://example.com"
        )
        assert opts_slash.cdn_url == opts_no_slash.cdn_url

    def test_metrics_url_derived_from_geo(self) -> None:
        opts = DynatraceProviderOptions.from_sdk_key(_VALID_KEY)
        assert opts.metrics_url == "https://metrics.us.fm.dynatrace.com/v1/metrics"

    def test_geo_reflected_in_metrics_url(self) -> None:
        eu_key = "dt01.server_eu_abcdef1234.de848e97a9cc4cc78aae568e65f49a9d_a1b2c3d4e5"
        opts = DynatraceProviderOptions.from_sdk_key(eu_key)
        assert opts.metrics_url == "https://metrics.eu.fm.dynatrace.com/v1/metrics"

    def test_metrics_origin_override(self) -> None:
        opts = DynatraceProviderOptions.from_sdk_key(
            _VALID_KEY, metrics_origin="https://metrics.us.fm.dynatracelabs.com"
        )
        assert opts.metrics_url == (
            "https://metrics.us.fm.dynatracelabs.com/v1/metrics"
        )

    def test_metrics_origin_trailing_slash_normalised(self) -> None:
        opts_slash = DynatraceProviderOptions.from_sdk_key(
            _VALID_KEY, metrics_origin="https://example.com/"
        )
        opts_no_slash = DynatraceProviderOptions.from_sdk_key(
            _VALID_KEY, metrics_origin="https://example.com"
        )
        assert opts_slash.metrics_url == opts_no_slash.metrics_url
        assert opts_slash.metrics_url == "https://example.com/v1/metrics"

    @pytest.mark.parametrize("field", ["config_origin", "metrics_origin"])
    def test_blank_origin_raises(self, field: str) -> None:
        with pytest.raises(ValueError, match="blank"):
            DynatraceProviderOptions.from_sdk_key(_VALID_KEY, **{field: "  "})

    @pytest.mark.parametrize("field", ["config_origin", "metrics_origin"])
    @pytest.mark.parametrize(
        "value",
        [
            "https://example.com/server",
            "https://example.com/v1/metrics",
            "https://example.com//",
            "https://example.com/..",
        ],
    )
    def test_origin_with_path_raises(self, field: str, value: str) -> None:
        with pytest.raises(ValueError, match="path"):
            DynatraceProviderOptions.from_sdk_key(_VALID_KEY, **{field: value})
