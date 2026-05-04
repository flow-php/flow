<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle;

use Flow\Bridge\Symfony\TelemetryBundle\DependencyInjection\Compiler\{CacheTelemetryPass, DBALTelemetryPass, HttpClientTelemetryPass, MainLoggerPass, OTLPAvailabilityPass, Psr18ClientTelemetryPass};
use Symfony\Component\DependencyInjection\Compiler\PassConfig;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpKernel\Bundle\Bundle;

final class FlowTelemetryBundle extends Bundle
{
    private const string CACHE_ADAPTER_INTERFACE = 'Symfony\\Component\\Cache\\Adapter\\AdapterInterface';

    private const string DBAL_MIDDLEWARE_INTERFACE = 'Doctrine\\DBAL\\Driver\\Middleware';

    private const string HTTP_CLIENT_INTERFACE = 'Symfony\\Contracts\\HttpClient\\HttpClientInterface';

    private const string PSR18_CLIENT_INTERFACE = 'Psr\\Http\\Client\\ClientInterface';

    private const string PSR18_TRACEABLE_CLIENT = 'Flow\\Bridge\\Psr18\\Telemetry\\PSR18TraceableClient';

    public function build(ContainerBuilder $container) : void
    {
        parent::build($container);

        $container->addCompilerPass(new OTLPAvailabilityPass());
        $container->addCompilerPass(new MainLoggerPass(), PassConfig::TYPE_BEFORE_OPTIMIZATION, -64);

        if (\interface_exists(self::HTTP_CLIENT_INTERFACE)) {
            $container->addCompilerPass(new HttpClientTelemetryPass());
        }

        if (\interface_exists(self::PSR18_CLIENT_INTERFACE) && \class_exists(self::PSR18_TRACEABLE_CLIENT)) {
            $container->addCompilerPass(new Psr18ClientTelemetryPass());
        }

        if (\interface_exists(self::DBAL_MIDDLEWARE_INTERFACE)) {
            $container->addCompilerPass(new DBALTelemetryPass());
        }

        if (\interface_exists(self::CACHE_ADAPTER_INTERFACE)) {
            $container->addCompilerPass(new CacheTelemetryPass());
        }
    }
}
