<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Tests\Integration;

use Flow\Bridge\Telemetry\OTLP\Tests\Context\OtelContext;
use Flow\Bridge\Telemetry\OTLP\Tests\Context\TransportConfiguration;
use Generator;
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected OtelContext $otelContext;

    public static function transportProvider(): Generator
    {
        foreach (TransportConfiguration::available() as $config) {
            yield $config->name => [$config];
        }
    }

    protected function setUp(): void
    {
        $this->otelContext = OtelContext::instance();
    }
}
