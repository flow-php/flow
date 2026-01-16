<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Integration;

use Flow\Bridge\Telemetry\OTLP\Tests\Context\{OtelContext, TransportConfiguration};
use PHPUnit\Framework\TestCase;

/**
 * Base class for integration tests that verify telemetry data reaches the collector.
 *
 * Uses OtelContext to access shared test infrastructure and CollectorMetrics
 * to verify that telemetry was received by the collector.
 */
abstract class IntegrationTestCase extends TestCase
{
    protected OtelContext $otelContext;

    /**
     * Data provider that yields all available transport/serializer configurations.
     *
     * Configurations are automatically skipped if required extensions are not available.
     */
    public static function transportProvider() : \Generator
    {
        foreach (TransportConfiguration::available() as $config) {
            yield $config->name => [$config];
        }
    }

    protected function setUp() : void
    {
        $this->otelContext = OtelContext::instance();
    }
}
