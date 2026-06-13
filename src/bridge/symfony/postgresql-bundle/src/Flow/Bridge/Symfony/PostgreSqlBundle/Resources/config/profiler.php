<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\FlowPostgreSqlDataCollector;
use Flow\PostgreSql\Client\Debug\QueryLog;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\param;
use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services->set('flow.postgresql.profiler.query_log', QueryLog::class)->public();

    $services->set('flow.postgresql.profiler.collector', FlowPostgreSqlDataCollector::class)->args([
        service('flow.postgresql.profiler.query_log'),
        param('flow.postgresql.profiler.include_parameters'),
    ])->tag('data_collector', [
        'id' => 'flow_postgresql',
        'template' => '@FlowPostgreSql/Collector/postgresql.html.twig',
        'priority' => 240,
    ]);
};
