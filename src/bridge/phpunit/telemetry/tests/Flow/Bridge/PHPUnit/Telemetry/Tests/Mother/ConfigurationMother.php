<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry\Tests\Mother;

use Flow\Bridge\PHPUnit\Telemetry\Configuration;

final class ConfigurationMother
{
    public static function default() : Configuration
    {
        return new Configuration(
            serviceName: 'phpunit',
            otelCollectorUrl: 'http://localhost:4318',
            emitTraces: true,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
        );
    }

    public static function withCustomServiceName(string $serviceName) : Configuration
    {
        return new Configuration(
            serviceName: $serviceName,
            otelCollectorUrl: 'http://localhost:4318',
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
            otelCollectorUrl: $url,
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
            otelCollectorUrl: 'http://localhost:4318',
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
            otelCollectorUrl: 'http://localhost:4318',
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
            otelCollectorUrl: 'http://localhost:4318',
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
            otelCollectorUrl: 'http://localhost:4318',
            emitTraces: false,
            emitMetrics: true,
            emitTestSpans: true,
            emitTestCaseSpans: true,
        );
    }
}
