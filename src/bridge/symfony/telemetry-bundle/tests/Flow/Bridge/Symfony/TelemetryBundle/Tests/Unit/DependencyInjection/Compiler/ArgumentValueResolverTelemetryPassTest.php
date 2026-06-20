<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\ArgumentValueResolverTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\HttpKernel\TracingValueResolver;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Definition;

#[CoversClass(ArgumentValueResolverTelemetryPass::class)]
final class ArgumentValueResolverTelemetryPassTest extends TestCase
{
    public function test_no_op_when_parameter_absent(): void
    {
        $container = new ContainerBuilder();
        $container->setDefinition(
            'value_resolver.test',
            (new Definition())->addTag('controller.argument_value_resolver'),
        );

        (new ArgumentValueResolverTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('value_resolver.test.flow_telemetry'));
    }

    public function test_no_op_when_parameter_false(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.http_kernel.trace_controller_argument_resolvers', false);
        $container->setDefinition(
            'value_resolver.test',
            (new Definition())->addTag('controller.argument_value_resolver'),
        );

        (new ArgumentValueResolverTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('value_resolver.test.flow_telemetry'));
    }

    public function test_decorates_concrete_tagged_resolvers_only(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.http_kernel.trace_controller_argument_resolvers', true);
        $container->setDefinition(
            'value_resolver.one',
            (new Definition())->addTag('controller.argument_value_resolver'),
        );
        $container->setDefinition(
            'value_resolver.two',
            (new Definition())->addTag('controller.argument_value_resolver'),
        );
        $container->setDefinition(
            'value_resolver.abstract',
            (new Definition())
                ->setAbstract(true)
                ->addTag('controller.argument_value_resolver'),
        );

        (new ArgumentValueResolverTelemetryPass())->process($container);

        static::assertFalse($container->hasDefinition('value_resolver.abstract.flow_telemetry'));

        foreach (['value_resolver.one', 'value_resolver.two'] as $serviceId) {
            $decorator = $container->getDefinition($serviceId . '.flow_telemetry');
            static::assertSame(TracingValueResolver::class, $decorator->getClass());

            $decorated = $decorator->getDecoratedService();
            static::assertNotNull($decorated);
            static::assertSame($serviceId, $decorated[0]);
            static::assertSame($serviceId . '.flow_telemetry.inner', (string) $decorator->getArgument(0));
        }
    }
}
