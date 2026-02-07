<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\{HttpClientTelemetryPass, OTLPAvailabilityPass};
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\Bundle;
use Symfony\Contracts\HttpClient\HttpClientInterface;

final class FlowTelemetryBundle extends Bundle
{
    public function build(ContainerBuilder $container) : void
    {
        parent::build($container);

        $container->addCompilerPass(new OTLPAvailabilityPass());

        if (\interface_exists(HttpClientInterface::class)) {
            $container->addCompilerPass(new HttpClientTelemetryPass());
        }
    }
}
