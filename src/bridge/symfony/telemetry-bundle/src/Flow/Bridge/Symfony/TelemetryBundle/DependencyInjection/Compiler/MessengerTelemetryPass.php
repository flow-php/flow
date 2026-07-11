<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

use function array_keys;
use function array_unshift;

/**
 * Injects the messenger TracingMiddleware into every message bus as the outermost middleware, so consumed
 * and produced messages are traced without the application having to register it manually. Runs before
 * Symfony's MessengerPass, which turns each "<busId>.middleware" parameter into the bus's middleware stack.
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
