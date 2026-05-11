<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\TelemetryBundle\Exception\RuntimeException;
use Symfony\Component\DependencyInjection\Compiler\CompilerPassInterface;
use Symfony\Component\DependencyInjection\ContainerBuilder;

final class OTLPAvailabilityPass implements CompilerPassInterface
{
    private const string OTLP_BRIDGE_CLASS = 'Flow\\Bridge\\Telemetry\\OTLP\\Exporter\\OTLPExporter';

    public function process(ContainerBuilder $container): void
    {
        $otlpAvailable = \class_exists(self::OTLP_BRIDGE_CLASS);
        $container->setParameter('flow.telemetry.otlp_available', $otlpAvailable);

        $otlpConfigured =
            $container->hasParameter('flow.telemetry.otlp_configured')
            && $container->getParameter('flow.telemetry.otlp_configured') === true;

        if ($otlpConfigured && !$otlpAvailable) {
            throw new RuntimeException(
                'OTLP exporter is configured but the flow-php/telemetry-otlp-bridge package is not installed. '
                . 'Please install it with: composer require flow-php/telemetry-otlp-bridge',
            );
        }
    }
}
