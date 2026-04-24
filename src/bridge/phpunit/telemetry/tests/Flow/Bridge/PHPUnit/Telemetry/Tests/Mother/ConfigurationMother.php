<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Mother;

use Flow\Bridge\PHPUnit\Telemetry\{Configuration, CurlTransportConfig, SerializerType};

final class ConfigurationMother
{
    public static function default() : Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport(),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
        );
    }

    public static function defaultTransport(string $endpoint = Configuration::DEFAULT_ENDPOINT) : CurlTransportConfig
    {
        return new CurlTransportConfig(
            endpoint: $endpoint,
            headers: [],
            timeout: 30,
            connectTimeout: 10,
            compression: false,
            followRedirects: true,
            maxRedirects: 3,
            proxy: null,
            sslVerifyPeer: true,
            sslVerifyHost: true,
            sslCertPath: null,
            sslKeyPath: null,
            caInfoPath: null,
            serializer: SerializerType::JSON,
        );
    }

    public static function withCustomServiceName(string $serviceName) : Configuration
    {
        return new Configuration(
            serviceName: $serviceName,
            transport: self::defaultTransport(),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
        );
    }

    public static function withCustomUrl(string $url) : Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport($url),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
        );
    }

    public static function withDisabledMetrics() : Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport(),
            emitTraces: true,
            emitMetrics: false,
            emitTestSpans: true,
            emitTestCaseSpans: true,
        );
    }

    public static function withDisabledTestCaseSpans() : Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport(),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: false,
        );
    }

    public static function withDisabledTestSpans() : Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport(),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: false,
            emitTestCaseSpans: true,
        );
    }

    public static function withDisabledTraces() : Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport(),
            emitTraces: false,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
        );
    }
}
