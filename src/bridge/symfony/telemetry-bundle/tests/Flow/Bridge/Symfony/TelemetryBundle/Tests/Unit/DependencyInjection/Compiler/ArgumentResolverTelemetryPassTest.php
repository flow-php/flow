<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ArgumentResolverTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\TracingArgumentResolver;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

#[CoversClass(ArgumentResolverTelemetryPass::class)]
final class ArgumentResolverTelemetryPassTest extends TestCase
{
    public function test_no_op_when_parameter_absent(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition('argument_resolver', new Definition());

        (new ArgumentResolverTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('argument_resolver.flow_telemetry'));
    }

    public function test_no_op_when_parameter_false(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.http_kernel.trace_controller_arguments', false);
        $container->setDefinition('argument_resolver', new Definition());

        (new ArgumentResolverTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('argument_resolver.flow_telemetry'));
    }

    public function test_decorates_argument_resolver_when_enabled(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.http_kernel.trace_controller_arguments', true);
        $container->setDefinition('argument_resolver', new Definition());

        (new ArgumentResolverTelemetryPass())->process($container);

        $decorator = $container->getDefinition('argument_resolver.flow_telemetry');
        static::assertSame(TracingArgumentResolver::class, $decorator->getClass());

        $decorated = $decorator->getDecoratedService();
        static::assertNotNull($decorated);
        static::assertSame('argument_resolver', $decorated[0]);
        static::assertSame('argument_resolver.flow_telemetry.inner', (string) $decorator->getArgument(0));
    }
}
