<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Doctrine\DBAL\Driver\Middleware as DBALMiddlewareInterface;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\DBALTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\Instrumentation\Doctrine\DBAL\TransactionSpanMode;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function interface_exists;

#[CoversClass(DBALTelemetryPass::class)]
final class DBALTelemetryPassTest extends TestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(DBALMiddlewareInterface::class)) {
            self::markTestSkipped('doctrine/dbal is not installed');
        }
    }

    public function test_transaction_spans_mode_is_wired_into_the_middleware_definition(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.dbal.enabled', true);
        $container->setParameter('flow.telemetry.dbal.transaction_spans', 'per_operation');
        $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);

        (new DBALTelemetryPass())->process($container);

        static::assertSame(
            TransactionSpanMode::PER_OPERATION,
            $container->getDefinition('flow.telemetry.dbal.middleware.default')->getArgument(5),
        );
    }

    public function test_transaction_spans_mode_defaults_to_grouped_when_parameter_absent(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.dbal.enabled', true);
        $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);

        (new DBALTelemetryPass())->process($container);

        static::assertSame(
            TransactionSpanMode::GROUPED,
            $container->getDefinition('flow.telemetry.dbal.middleware.default')->getArgument(5),
        );
    }

    public function test_metric_and_parameter_config_is_wired_into_the_middleware_definition(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.dbal.enabled', true);
        $container->setParameter('flow.telemetry.dbal.collect_metrics', false);
        $container->setParameter('flow.telemetry.dbal.include_parameters', true);
        $container->setParameter('flow.telemetry.dbal.max_parameters', 3);
        $container->setParameter('flow.telemetry.dbal.max_parameter_length', 20);
        $container->setParameter('doctrine.connections', ['default' => 'doctrine.dbal.default_connection']);

        (new DBALTelemetryPass())->process($container);

        $definition = $container->getDefinition('flow.telemetry.dbal.middleware.default');
        static::assertFalse($definition->getArgument(6), 'collect_metrics');
        static::assertTrue($definition->getArgument(7), 'include_parameters');
        static::assertSame(3, $definition->getArgument(8), 'max_parameters');
        static::assertSame(20, $definition->getArgument(9), 'max_parameter_length');
    }
}
