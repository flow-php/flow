<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\FlowMigrationsDataCollector;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\param;
use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services
        ->set('flow.postgresql.profiler.migrations_collector', FlowMigrationsDataCollector::class)
        ->args([
            param('flow.postgresql.migrations.connection'),
            service('flow.postgresql.migrations.migrator'),
            service('flow.postgresql.migrations.configuration'),
        ])
        ->public()
        ->tag('data_collector', [
            'id' => 'flow_postgresql_migrations',
            'template' => '@FlowPostgreSql/Collector/migrations.html.twig',
            'priority' => 239,
        ]);
};
