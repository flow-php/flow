<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\OTLPAvailabilityPass;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\Bundle;

final class FlowTelemetryBundle extends Bundle
{
    public function build(ContainerBuilder $container) : void
    {
        parent::build($container);

        $container->addCompilerPass(new OTLPAvailabilityPass());
    }
}
