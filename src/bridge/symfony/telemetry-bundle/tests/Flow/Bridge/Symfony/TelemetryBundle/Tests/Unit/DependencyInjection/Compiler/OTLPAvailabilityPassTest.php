<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\OTLPAvailabilityPass;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;

#[CoversClass(OTLPAvailabilityPass::class)]
final class OTLPAvailabilityPassTest extends TestCase
{
    public function test_does_not_set_otlp_available_when_telemetry_is_disabled(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.enabled', false);

        (new OTLPAvailabilityPass())->process($container);

        static::assertFalse($container->hasParameter('flow.telemetry.otlp_available'));
    }

    public function test_sets_otlp_available_when_telemetry_is_enabled(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.enabled', true);

        (new OTLPAvailabilityPass())->process($container);

        static::assertTrue($container->hasParameter('flow.telemetry.otlp_available'));
    }

    public function test_sets_otlp_available_when_enabled_parameter_is_absent(): void
    {
        $container = new ContainerBuilder();

        (new OTLPAvailabilityPass())->process($container);

        static::assertTrue($container->hasParameter('flow.telemetry.otlp_available'));
    }
}
