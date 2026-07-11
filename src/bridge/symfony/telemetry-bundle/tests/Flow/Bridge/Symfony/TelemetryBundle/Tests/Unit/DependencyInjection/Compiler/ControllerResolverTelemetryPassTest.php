<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ControllerResolverTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\TracingControllerResolver;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

#[CoversClass(ControllerResolverTelemetryPass::class)]
final class ControllerResolverTelemetryPassTest extends TestCase
{
    public function test_no_op_when_parameter_absent(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('controller_resolver', new Definition());

        (new ControllerResolverTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('controller_resolver.flow_telemetry'));
    }

    public function test_no_op_when_parameter_false(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.http_kernel.trace_controller_resolution', false);
        $container->setDefinition('controller_resolver', new Definition());

        (new ControllerResolverTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('controller_resolver.flow_telemetry'));
    }

    public function test_decorates_controller_resolver_when_enabled(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.http_kernel.trace_controller_resolution', true);
        $container->setDefinition('controller_resolver', new Definition());

        (new ControllerResolverTelemetryPass())->process($container);

        $decorator = $container->getDefinition('controller_resolver.flow_telemetry');
        static::assertSame(TracingControllerResolver::class, $decorator->getClass());

        $decorated = $decorator->getDecoratedService();
        static::assertNotNull($decorated);
        static::assertSame('controller_resolver', $decorated[0]);
        static::assertSame('controller_resolver.flow_telemetry.inner', (string) $decorator->getArgument(0));
    }
}
