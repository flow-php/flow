<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function array_keys;
use function array_unshift;

/**
 * Must run before MessengerPass, which turns `<busId>.middleware` into the stack.
 */
final class MessengerTelemetryPass implements CompilerPassInterface
{
    public function process(ContainerBuilder $container): void
    {
        if (!$container->hasDefinition('flow.telemetry.messenger.middleware')) {
            return;
        }

        foreach (array_keys($container->findTaggedServiceIds('messenger.bus')) as $busId) {
            $parameter = $busId . '.middleware';

            if (!$container->hasParameter($parameter)) {
                continue;
            }

            /** @var list<array{id: string, arguments?: array<mixed>}> $middleware */
            $middleware = $container->getParameter($parameter);
            array_unshift($middleware, ['id' => 'flow.telemetry.messenger.middleware']);
            $container->setParameter($parameter, $middleware);
        }
    }
}
