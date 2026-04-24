<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Unit;

use Flow\Bridge\PHPUnit\Telemetry\{Configuration, CurlTransportConfig, GrpcTransportConfig, SerializerType};
use Flow\Bridge\PHPUnit\Telemetry\Tests\Context\DeprecationCapture;
use PHPUnit\Framework\TestCase;
use PHPUnit\Runner\Extension\ParameterCollection;

final class ConfigurationTest extends TestCase
{
    public function test_curl_compression_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'curl_compression' => 'true',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertTrue($config->transport->compression);
    }

    public function test_curl_endpoint_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'transport' => 'curl',
            'endpoint' => 'https://collector:4318',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame('https://collector:4318', $config->transport->endpoint);
    }

    public function test_curl_follow_redirects_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'curl_follow_redirects' => 'false',
            'curl_max_redirects' => '7',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertFalse($config->transport->followRedirects);
        self::assertSame(7, $config->transport->maxRedirects);
    }

    public function test_curl_headers_duplicate_name_last_wins() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'headers' => 'X-Token=first,X-Token=second',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame(['X-Token' => 'second'], $config->transport->headers);
    }

    public function test_curl_headers_empty_name_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Header name cannot be empty');

        Configuration::fromParameters(ParameterCollection::fromArray([
            'headers' => '=value',
        ]));
    }

    public function test_curl_headers_empty_string_is_empty_array() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'headers' => '',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame([], $config->transport->headers);
    }

    public function test_curl_headers_missing_equals_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid header entry "Authorization", expected format "name=value"');

        Configuration::fromParameters(ParameterCollection::fromArray([
            'headers' => 'Authorization',
        ]));
    }

    public function test_curl_headers_multiple_pairs_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'headers' => 'Authorization=Bearer xxx,X-Scope-OrgID=tenant-1',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame(
            ['Authorization' => 'Bearer xxx', 'X-Scope-OrgID' => 'tenant-1'],
            $config->transport->headers,
        );
    }

    public function test_curl_headers_single_pair_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'headers' => 'Authorization=Bearer xxx',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame(['Authorization' => 'Bearer xxx'], $config->transport->headers);
    }

    public function test_curl_headers_url_encoded_value_decoded() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'headers' => 'Authorization=Bearer%20xxx%2Cextra',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame(['Authorization' => 'Bearer xxx,extra'], $config->transport->headers);
    }

    public function test_curl_proxy_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'curl_proxy' => 'http://proxy:8080',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame('http://proxy:8080', $config->transport->proxy);
    }

    public function test_curl_serializer_defaults_to_json() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame(SerializerType::JSON, $config->transport->serializer);
    }

    public function test_curl_serializer_invalid_value_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid serializer "xml" for parameter "curl_serializer"');

        Configuration::fromParameters(ParameterCollection::fromArray([
            'curl_serializer' => 'xml',
        ]));
    }

    public function test_curl_serializer_protobuf_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'curl_serializer' => 'protobuf',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame(SerializerType::PROTOBUF, $config->transport->serializer);
    }

    public function test_curl_ssl_options_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'curl_ssl_verify_peer' => 'false',
            'curl_ssl_verify_host' => 'false',
            'curl_ssl_cert_path' => '/path/to/cert.pem',
            'curl_ssl_key_path' => '/path/to/key.pem',
            'curl_ca_info_path' => '/path/to/ca.pem',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertFalse($config->transport->sslVerifyPeer);
        self::assertFalse($config->transport->sslVerifyHost);
        self::assertSame('/path/to/cert.pem', $config->transport->sslCertPath);
        self::assertSame('/path/to/key.pem', $config->transport->sslKeyPath);
        self::assertSame('/path/to/ca.pem', $config->transport->caInfoPath);
    }

    public function test_curl_timeout_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'curl_timeout' => '120',
            'curl_connect_timeout' => '45',
        ]));

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame(120, $config->transport->timeout);
        self::assertSame(45, $config->transport->connectTimeout);
    }

    public function test_curl_with_grpc_specific_param_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Parameter "grpc_insecure" cannot be used with transport "curl"');

        Configuration::fromParameters(ParameterCollection::fromArray([
            'transport' => 'curl',
            'grpc_insecure' => 'true',
        ]));
    }

    public function test_default_configuration_uses_curl_transport_with_localhost_endpoint() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([]));

        self::assertSame('phpunit', $config->serviceName);
        self::assertTrue($config->emitTraces);
        self::assertTrue($config->emitMetrics);
        self::assertTrue($config->emitTestSpans);
        self::assertTrue($config->emitTestCaseSpans);

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame('http://localhost:4318', $config->transport->endpoint);
        self::assertSame([], $config->transport->headers);
        self::assertSame(30, $config->transport->timeout);
        self::assertSame(10, $config->transport->connectTimeout);
        self::assertFalse($config->transport->compression);
        self::assertTrue($config->transport->followRedirects);
        self::assertSame(3, $config->transport->maxRedirects);
        self::assertNull($config->transport->proxy);
        self::assertTrue($config->transport->sslVerifyPeer);
        self::assertTrue($config->transport->sslVerifyHost);
        self::assertNull($config->transport->sslCertPath);
        self::assertNull($config->transport->sslKeyPath);
        self::assertNull($config->transport->caInfoPath);
        self::assertSame(SerializerType::JSON, $config->transport->serializer);
    }

    public function test_emit_flags_can_be_disabled() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'emit_traces' => 'false',
            'emit_metrics' => 'false',
            'emit_test_spans' => 'false',
            'emit_test_case_spans' => 'false',
        ]));

        self::assertFalse($config->emitTraces);
        self::assertFalse($config->emitMetrics);
        self::assertFalse($config->emitTestSpans);
        self::assertFalse($config->emitTestCaseSpans);
    }

    public function test_empty_env_superglobal_treated_as_unset() : void
    {
        $_ENV['FLOW_PHPUNIT_OTEL_ENDPOINT'] = '';

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([
                'endpoint' => 'https://xml:4318',
            ]));

            self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
            self::assertSame('https://xml:4318', $config->transport->endpoint);
        } finally {
            unset($_ENV['FLOW_PHPUNIT_OTEL_ENDPOINT']);
        }
    }

    public function test_empty_env_var_treated_as_unset() : void
    {
        \putenv('FLOW_PHPUNIT_OTEL_ENDPOINT=');

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([
                'endpoint' => 'https://xml:4318',
            ]));

            self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
            self::assertSame('https://xml:4318', $config->transport->endpoint);
        } finally {
            \putenv('FLOW_PHPUNIT_OTEL_ENDPOINT');
        }
    }

    public function test_env_superglobal_wins_over_server_superglobal() : void
    {
        $_ENV['FLOW_PHPUNIT_OTEL_ENDPOINT'] = 'https://from-env:4318';
        $_SERVER['FLOW_PHPUNIT_OTEL_ENDPOINT'] = 'https://from-server:4318';

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([]));

            self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
            self::assertSame('https://from-env:4318', $config->transport->endpoint);
        } finally {
            unset($_ENV['FLOW_PHPUNIT_OTEL_ENDPOINT'], $_SERVER['FLOW_PHPUNIT_OTEL_ENDPOINT']);
        }
    }

    public function test_env_var_alone_used_when_xml_parameter_absent() : void
    {
        \putenv('FLOW_PHPUNIT_OTEL_ENDPOINT=https://env-collector:4318');

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([]));

            self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
            self::assertSame('https://env-collector:4318', $config->transport->endpoint);
        } finally {
            \putenv('FLOW_PHPUNIT_OTEL_ENDPOINT');
        }
    }

    public function test_env_var_boolean_parsing_supports_false_and_0() : void
    {
        \putenv('FLOW_PHPUNIT_OTEL_EMIT_METRICS=0');
        \putenv('FLOW_PHPUNIT_OTEL_EMIT_TRACES=false');

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([]));

            self::assertFalse($config->emitMetrics);
            self::assertFalse($config->emitTraces);
        } finally {
            \putenv('FLOW_PHPUNIT_OTEL_EMIT_METRICS');
            \putenv('FLOW_PHPUNIT_OTEL_EMIT_TRACES');
        }
    }

    public function test_env_var_headers_invalid_format_throws() : void
    {
        \putenv('FLOW_PHPUNIT_OTEL_HEADERS=broken');

        try {
            $this->expectException(\InvalidArgumentException::class);
            $this->expectExceptionMessage('Invalid header entry "broken"');

            Configuration::fromParameters(ParameterCollection::fromArray([]));
        } finally {
            \putenv('FLOW_PHPUNIT_OTEL_HEADERS');
        }
    }

    public function test_env_var_headers_parsed_same_as_xml() : void
    {
        \putenv('FLOW_PHPUNIT_OTEL_HEADERS=Authorization=Bearer%20env-token');

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([]));

            self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
            self::assertSame(['Authorization' => 'Bearer env-token'], $config->transport->headers);
        } finally {
            \putenv('FLOW_PHPUNIT_OTEL_HEADERS');
        }
    }

    public function test_env_var_legacy_collector_url_emits_deprecation() : void
    {
        \putenv('FLOW_PHPUNIT_OTEL_COLLECTOR_URL=http://env-legacy:4318');

        try {
            $captured = DeprecationCapture::around(
                static fn () => Configuration::fromParameters(ParameterCollection::fromArray([])),
            );

            self::assertInstanceOf(CurlTransportConfig::class, $captured['result']->transport);
            self::assertSame('http://env-legacy:4318', $captured['result']->transport->endpoint);
            self::assertNotNull($captured['message']);
            self::assertStringContainsString('otel_collector_url', $captured['message']);
        } finally {
            \putenv('FLOW_PHPUNIT_OTEL_COLLECTOR_URL');
        }
    }

    public function test_env_var_overrides_xml_parameter_for_service_name() : void
    {
        \putenv('FLOW_PHPUNIT_OTEL_SERVICE_NAME=env-suite');

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([
                'service_name' => 'xml-suite',
            ]));

            self::assertSame('env-suite', $config->serviceName);
        } finally {
            \putenv('FLOW_PHPUNIT_OTEL_SERVICE_NAME');
        }
    }

    public function test_env_var_read_from_env_superglobal() : void
    {
        $_ENV['FLOW_PHPUNIT_OTEL_ENDPOINT'] = 'https://from-env-superglobal:4318';

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([]));

            self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
            self::assertSame('https://from-env-superglobal:4318', $config->transport->endpoint);
        } finally {
            unset($_ENV['FLOW_PHPUNIT_OTEL_ENDPOINT']);
        }
    }

    public function test_env_var_read_from_server_superglobal() : void
    {
        $_SERVER['FLOW_PHPUNIT_OTEL_ENDPOINT'] = 'https://from-server-superglobal:4318';

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([]));

            self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
            self::assertSame('https://from-server-superglobal:4318', $config->transport->endpoint);
        } finally {
            unset($_SERVER['FLOW_PHPUNIT_OTEL_ENDPOINT']);
        }
    }

    public function test_env_var_transport_selects_grpc_when_xml_says_curl() : void
    {
        \putenv('FLOW_PHPUNIT_OTEL_TRANSPORT=grpc');

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([
                'transport' => 'curl',
                'endpoint' => 'otel:4317',
            ]));

            self::assertInstanceOf(GrpcTransportConfig::class, $config->transport);
        } finally {
            \putenv('FLOW_PHPUNIT_OTEL_TRANSPORT');
        }
    }

    public function test_grpc_headers_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'transport' => 'grpc',
            'endpoint' => 'otel:4317',
            'headers' => 'api-key=secret',
        ]));

        self::assertInstanceOf(GrpcTransportConfig::class, $config->transport);
        self::assertSame(['api-key' => 'secret'], $config->transport->headers);
    }

    public function test_grpc_insecure_false_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'transport' => 'grpc',
            'endpoint' => 'otel:4317',
            'grpc_insecure' => 'false',
        ]));

        self::assertInstanceOf(GrpcTransportConfig::class, $config->transport);
        self::assertFalse($config->transport->insecure);
    }

    public function test_grpc_transport_selected() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'transport' => 'grpc',
            'endpoint' => 'otel:4317',
        ]));

        self::assertInstanceOf(GrpcTransportConfig::class, $config->transport);
        self::assertSame('otel:4317', $config->transport->endpoint);
        self::assertSame([], $config->transport->headers);
        self::assertTrue($config->transport->insecure);
    }

    public function test_grpc_with_curl_specific_param_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Parameter "curl_compression" cannot be used with transport "grpc"');

        Configuration::fromParameters(ParameterCollection::fromArray([
            'transport' => 'grpc',
            'endpoint' => 'otel:4317',
            'curl_compression' => 'true',
        ]));
    }

    public function test_grpc_with_curl_timeout_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Parameter "curl_timeout" cannot be used with transport "grpc"');

        Configuration::fromParameters(ParameterCollection::fromArray([
            'transport' => 'grpc',
            'endpoint' => 'otel:4317',
            'curl_timeout' => '30',
        ]));
    }

    public function test_invalid_boolean_value_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid boolean value "yes" for parameter "emit_traces"');

        Configuration::fromParameters(ParameterCollection::fromArray([
            'emit_traces' => 'yes',
        ]));
    }

    public function test_invalid_integer_value_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid integer value "abc" for parameter "curl_timeout"');

        Configuration::fromParameters(ParameterCollection::fromArray([
            'curl_timeout' => 'abc',
        ]));
    }

    public function test_invalid_transport_value_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid transport "rest", expected "curl" or "grpc"');

        Configuration::fromParameters(ParameterCollection::fromArray([
            'transport' => 'rest',
        ]));
    }

    public function test_legacy_otel_collector_url_combined_with_new_endpoint_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Deprecated parameter "otel_collector_url" cannot be mixed with new parameter "endpoint"');

        DeprecationCapture::silence(
            static fn () => Configuration::fromParameters(ParameterCollection::fromArray([
                'otel_collector_url' => 'http://legacy:4318',
                'endpoint' => 'http://new:4318',
            ])),
        );
    }

    public function test_legacy_otel_collector_url_combined_with_transport_throws() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Deprecated parameter "otel_collector_url" cannot be mixed with new parameter "transport"');

        DeprecationCapture::silence(
            static fn () => Configuration::fromParameters(ParameterCollection::fromArray([
                'otel_collector_url' => 'http://legacy:4318',
                'transport' => 'curl',
            ])),
        );
    }

    public function test_legacy_otel_collector_url_maps_to_curl_endpoint() : void
    {
        $captured = DeprecationCapture::around(
            static fn () => Configuration::fromParameters(ParameterCollection::fromArray([
                'otel_collector_url' => 'http://legacy:4318',
            ])),
        );

        $config = $captured['result'];

        self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
        self::assertSame('http://legacy:4318', $config->transport->endpoint);
        self::assertNotNull($captured['message']);
        self::assertStringContainsString('otel_collector_url', $captured['message']);
    }

    public function test_server_superglobal_wins_over_getenv() : void
    {
        $_SERVER['FLOW_PHPUNIT_OTEL_ENDPOINT'] = 'https://from-server:4318';
        \putenv('FLOW_PHPUNIT_OTEL_ENDPOINT=https://from-getenv:4318');

        try {
            $config = Configuration::fromParameters(ParameterCollection::fromArray([]));

            self::assertInstanceOf(CurlTransportConfig::class, $config->transport);
            self::assertSame('https://from-server:4318', $config->transport->endpoint);
        } finally {
            unset($_SERVER['FLOW_PHPUNIT_OTEL_ENDPOINT']);
            \putenv('FLOW_PHPUNIT_OTEL_ENDPOINT');
        }
    }

    public function test_service_name_parsed() : void
    {
        $config = Configuration::fromParameters(ParameterCollection::fromArray([
            'service_name' => 'my-suite',
        ]));

        self::assertSame('my-suite', $config->serviceName);
    }
}
