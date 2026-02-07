<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\{DBALTelemetryPass, HttpClientTelemetryPass, OTLPAvailabilityPass};
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\Bundle;

final class FlowTelemetryBundle extends Bundle
{
    private const string DBAL_MIDDLEWARE_INTERFACE = 'Doctrine\\DBAL\\Driver\\Middleware';

    private const string HTTP_CLIENT_INTERFACE = 'Symfony\\Contracts\\HttpClient\\HttpClientInterface';

    public function build(ContainerBuilder $container) : void
    {
        parent::build($container);

        $container->addCompilerPass(new OTLPAvailabilityPass());

        if (\interface_exists(self::HTTP_CLIENT_INTERFACE)) {
            $container->addCompilerPass(new HttpClientTelemetryPass());
        }

        if (\interface_exists(self::DBAL_MIDDLEWARE_INTERFACE)) {
            $container->addCompilerPass(new DBALTelemetryPass());
        }
    }
}
