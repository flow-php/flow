<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

final class MainLoggerPass implements CompilerPassInterface
{
    private const string SYMFONY_DEFAULT_LOGGER = 'Symfony\\Component\\HttpKernel\\Log\\Logger';

    public function process(ContainerBuilder $container) : void
    {
        $mainLogger = $container->hasParameter('flow.telemetry.main_logger')
            ? $container->getParameter('flow.telemetry.main_logger')
            : null;

        if ($mainLogger !== null) {
            if (!\is_string($mainLogger) || $mainLogger === '') {
                throw new RuntimeException('flow_telemetry.main_logger must be a non-empty string referencing a configured logger name.');
            }

            $targetId = 'flow.telemetry.' . $mainLogger . '.logger.psr3';

            if (!$container->hasDefinition($targetId) && !$container->hasAlias($targetId)) {
                throw new RuntimeException(\sprintf(
                    'Configured main_logger "%s" does not have a registered PSR-3 wrapper service "%s". Make sure a logger with that name is configured under flow_telemetry.loggers, or use the always-available "default".',
                    $mainLogger,
                    $targetId,
                ));
            }

            $container->setAlias('logger', $targetId)->setPublic(true);

            return;
        }

        if ($container->hasAlias('logger')) {
            return;
        }

        if (!$container->hasDefinition('logger')) {
            return;
        }

        if ($container->getDefinition('logger')->getClass() !== self::SYMFONY_DEFAULT_LOGGER) {
            return;
        }

        $container->setAlias('logger', 'flow.telemetry.default.logger.psr3')->setPublic(true);
    }
}
