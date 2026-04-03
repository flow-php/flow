<?php

declare(strict_types=1);

use function Symfony\Component\DependencyInjection\Loader\Configurator\{param, service};

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\{CreateDatabaseCommand, DropDatabaseCommand, RunSqlCommand};
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

return static function (ContainerConfigurator $container) : void {
    $services = $container->services();

    $services->set('flow.postgresql.command.database_create', CreateDatabaseCommand::class)
        ->args([service('service_container'), param('flow.postgresql.default_connection')])
        ->tag('console.command');

    $services->set('flow.postgresql.command.database_drop', DropDatabaseCommand::class)
        ->args([service('service_container'), param('flow.postgresql.default_connection')])
        ->tag('console.command');

    $services->set('flow.postgresql.command.sql_run', RunSqlCommand::class)
        ->args([service('service_container'), param('flow.postgresql.default_connection')])
        ->tag('console.command');
};
