<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Mother;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;
use Flow\Bridge\PHPUnit\Telemetry\CurlTransportConfig;
use Flow\Bridge\PHPUnit\Telemetry\ErrorLogHandlerConfig;
use Flow\Bridge\PHPUnit\Telemetry\SerializerType;
use Flow\Telemetry\ErrorHandler\ErrorLogMessageType;

final class ConfigurationMother
{
    public static function default(): Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport(),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
            batchSize: Configuration::DEFAULT_BATCH_SIZE,
            errorHandler: self::defaultErrorHandler(),
        );
    }

    public static function defaultErrorHandler(): ErrorLogHandlerConfig
    {
        return new ErrorLogHandlerConfig(
            messageType: ErrorLogMessageType::OperatingSystem,
            expandNewlines: false,
            messagePrefix: Configuration::DEFAULT_MESSAGE_PREFIX,
        );
    }

    public static function defaultTransport(string $endpoint = Configuration::DEFAULT_ENDPOINT): CurlTransportConfig
    {
        return new CurlTransportConfig(
            endpoint: $endpoint,
            headers: [],
            timeoutMs: Configuration::DEFAULT_TIMEOUT_MS,
            connectTimeoutMs: Configuration::DEFAULT_CONNECT_TIMEOUT_MS,
            shutdownTimeoutMs: Configuration::DEFAULT_SHUTDOWN_TIMEOUT_MS,
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

    public static function withCustomServiceName(string $serviceName): Configuration
    {
        return new Configuration(
            serviceName: $serviceName,
            transport: self::defaultTransport(),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
            batchSize: Configuration::DEFAULT_BATCH_SIZE,
            errorHandler: self::defaultErrorHandler(),
        );
    }

    public static function withCustomUrl(string $url): Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport($url),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
            batchSize: Configuration::DEFAULT_BATCH_SIZE,
            errorHandler: self::defaultErrorHandler(),
        );
    }

    public static function withDisabledMetrics(): Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport(),
            emitTraces: true,
            emitMetrics: false,
            emitTestSpans: true,
            emitTestCaseSpans: true,
            batchSize: Configuration::DEFAULT_BATCH_SIZE,
            errorHandler: self::defaultErrorHandler(),
        );
    }

    public static function withDisabledTestCaseSpans(): Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport(),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: false,
            batchSize: Configuration::DEFAULT_BATCH_SIZE,
            errorHandler: self::defaultErrorHandler(),
        );
    }

    public static function withDisabledTestSpans(): Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport(),
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: false,
            emitTestCaseSpans: true,
            batchSize: Configuration::DEFAULT_BATCH_SIZE,
            errorHandler: self::defaultErrorHandler(),
        );
    }

    public static function withDisabledTraces(): Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            transport: self::defaultTransport(),
            emitTraces: false,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
            batchSize: Configuration::DEFAULT_BATCH_SIZE,
            errorHandler: self::defaultErrorHandler(),
        );
    }
}
