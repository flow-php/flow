<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\MessengerTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Messenger\TracingMiddleware;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

#[CoversClass(MessengerTelemetryPass::class)]
final class MessengerTelemetryPassTest extends TestCase
{
    public function test_prepends_the_tracing_middleware_to_every_bus(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('flow.telemetry.messenger.middleware', new Definition(TracingMiddleware::class));
        $container->register('messenger.bus.default')->addTag('messenger.bus');
        $container->register('messenger.bus.commands')->addTag('messenger.bus');
        $container->setParameter('messenger.bus.default.middleware', [['id' => 'add_bus_name_stamp_middleware']]);
        $container->setParameter('messenger.bus.commands.middleware', []);

        (new MessengerTelemetryPass())->process($container);

        static::assertSame(
            [['id' => 'flow.telemetry.messenger.middleware'], ['id' => 'add_bus_name_stamp_middleware']],
            $container->getParameter('messenger.bus.default.middleware'),
        );
        static::assertSame(
            [['id' => 'flow.telemetry.messenger.middleware']],
            $container->getParameter('messenger.bus.commands.middleware'),
        );
    }

    public function test_is_a_no_op_when_the_middleware_is_not_registered(): void
    {
        $container = new ContainerBuilder();
        $container->register('messenger.bus.default')->addTag('messenger.bus');
        $container->setParameter('messenger.bus.default.middleware', []);

        (new MessengerTelemetryPass())->process($container);

        static::assertSame([], $container->getParameter('messenger.bus.default.middleware'));
    }

    public function test_skips_buses_without_a_middleware_parameter(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('flow.telemetry.messenger.middleware', new Definition(TracingMiddleware::class));
        $container->register('messenger.bus.default')->addTag('messenger.bus');

        (new MessengerTelemetryPass())->process($container);

        static::assertFalse($container->hasParameter('messenger.bus.default.middleware'));
    }
}
