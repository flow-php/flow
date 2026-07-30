<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\FlowPostgreSqlDataCollector;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\QueryRecorder;
use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\QueryRecorderOptions;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\inline_service;
use function Symfony\Component\DependencyInjection\Loader\Configurator\param;
use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services
        ->set('flow.postgresql.profiler.query_recorder', QueryRecorder::class)
        ->args([
            inline_service(QueryRecorderOptions::class)->args([
                param('flow.postgresql.profiler.max_queries'),
                param('flow.postgresql.profiler.include_parameters'),
                param('flow.postgresql.profiler.max_retained_parameters'),
                param('flow.postgresql.profiler.max_query_length'),
            ]),
        ])
        ->public();

    $services->set('flow.postgresql.profiler.collector', FlowPostgreSqlDataCollector::class)->args([
        service('flow.postgresql.profiler.query_recorder'),
    ])->tag('data_collector', [
        'id' => 'flow_postgresql',
        'template' => '@FlowPostgreSql/Collector/postgresql.html.twig',
        'priority' => 240,
    ]);
};
