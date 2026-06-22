<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Unit\DependencyInjection\Compiler;

use Doctrine\DBAL\Driver\Middleware as DBALMiddlewareInterface;
use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\DBALTelemetryPass;
use Flow\Bridge\Symfony\TelemetryBundle\FlowTelemetryBundle;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\NoopCompilerPass;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function array_search;
use function interface_exists;

#[CoversClass(FlowTelemetryBundle::class)]
final class DBALTelemetryPassOrderingTest extends TestCase
{
    protected function setUp(): void
    {
        if (!interface_exists(DBALMiddlewareInterface::class)) {
            self::markTestSkipped('doctrine/dbal is not installed');
        }
    }

    public function test_dbal_pass_runs_before_a_priority_zero_pass_registered_before_the_bundle(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.dbal.enabled', true);

        $consumer = new NoopCompilerPass();
        $container->addCompilerPass($consumer, PassConfig::TYPE_BEFORE_OPTIMIZATION, 0);

        (new FlowTelemetryBundle())->build($container);

        $passes = $container->getCompiler()->getPassConfig()->getBeforeOptimizationPasses();
        $dbalIndex = null;
        $consumerIndex = array_search($consumer, $passes, true);

        foreach ($passes as $index => $pass) {
            if ($pass instanceof DBALTelemetryPass) {
                $dbalIndex = $index;
            }
        }

        static::assertNotNull($dbalIndex, 'DBALTelemetryPass was not registered by FlowTelemetryBundle::build().');
        static::assertNotFalse($consumerIndex, 'The priority-0 consumer pass was not registered.');
        static::assertLessThan(
            $consumerIndex,
            $dbalIndex,
            'DBALTelemetryPass must run before a priority-0 pass even when that pass (e.g. DoctrineBundle\'s '
            . 'MiddlewaresPass) is registered before FlowTelemetryBundle.',
        );
    }

    public function test_dbal_pass_runs_before_a_priority_zero_pass_registered_after_the_bundle(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter('flow.telemetry.dbal.enabled', true);

        (new FlowTelemetryBundle())->build($container);

        $consumer = new NoopCompilerPass();
        $container->addCompilerPass($consumer, PassConfig::TYPE_BEFORE_OPTIMIZATION, 0);

        $passes = $container->getCompiler()->getPassConfig()->getBeforeOptimizationPasses();
        $dbalIndex = null;
        $consumerIndex = array_search($consumer, $passes, true);

        foreach ($passes as $index => $pass) {
            if ($pass instanceof DBALTelemetryPass) {
                $dbalIndex = $index;
            }
        }

        static::assertNotNull($dbalIndex, 'DBALTelemetryPass was not registered by FlowTelemetryBundle::build().');
        static::assertNotFalse($consumerIndex, 'The priority-0 consumer pass was not registered.');
        static::assertLessThan(
            $consumerIndex,
            $dbalIndex,
            'DBALTelemetryPass ordering must hold regardless of bundle registration order.',
        );
    }
}
