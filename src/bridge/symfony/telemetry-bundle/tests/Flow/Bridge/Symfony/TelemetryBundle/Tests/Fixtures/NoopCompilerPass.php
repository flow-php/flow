<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures;

use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

/**
 * Stand-in for a third-party priority-0 TYPE_BEFORE_OPTIMIZATION pass (such as DoctrineBundle's
 * MiddlewaresPass) used to pin DBALTelemetryPass execution order in DBALTelemetryPassOrderingTest.
 */
final class NoopCompilerPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void {}
}
