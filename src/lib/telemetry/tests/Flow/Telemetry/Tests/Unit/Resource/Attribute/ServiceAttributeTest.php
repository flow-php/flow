<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Attribute;

use Flow\Telemetry\Resource\Attribute\ServiceAttribute;
use PHPUnit\Framework\TestCase;

final class ServiceAttributeTest extends TestCase
{
    public function test_service_namespace_case_holds_the_otel_key(): void
    {
        static::assertSame('service.namespace', ServiceAttribute::SERVICE_NAMESPACE->value);
    }
}
