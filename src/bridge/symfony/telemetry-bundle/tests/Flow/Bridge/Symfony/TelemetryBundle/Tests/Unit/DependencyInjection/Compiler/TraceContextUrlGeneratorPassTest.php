<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\TraceContextUrlGeneratorPass;
use Flow\Bridge\Symfony\TelemetryBundle\Routing\TraceContextUrlGenerator;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use stdClass;
use Symfony\Component\DependencyInjection\ContainerBuilder;

#[CoversClass(TraceContextUrlGeneratorPass::class)]
final class TraceContextUrlGeneratorPassTest extends TestCase
{
    public function test_does_nothing_when_generator_definition_is_absent(): void
    {
        $container = new ContainerBuilder();

        (new TraceContextUrlGeneratorPass())->process($container);

        static::assertFalse($container->hasDefinition('flow.telemetry.trace_context_url_generator'));
    }

    public function test_keeps_generator_when_router_is_present(): void
    {
        $container = new ContainerBuilder();
        $container->register('flow.telemetry.trace_context_url_generator', stdClass::class);
        $container->register('router', stdClass::class);

        (new TraceContextUrlGeneratorPass())->process($container);

        static::assertTrue($container->hasDefinition('flow.telemetry.trace_context_url_generator'));
    }

    public function test_removes_generator_when_router_is_absent(): void
    {
        $container = new ContainerBuilder();
        $container->register('flow.telemetry.trace_context_url_generator', stdClass::class);
        $container->setAlias(TraceContextUrlGenerator::class, 'flow.telemetry.trace_context_url_generator');

        (new TraceContextUrlGeneratorPass())->process($container);

        static::assertFalse($container->hasDefinition('flow.telemetry.trace_context_url_generator'));
    }
}
