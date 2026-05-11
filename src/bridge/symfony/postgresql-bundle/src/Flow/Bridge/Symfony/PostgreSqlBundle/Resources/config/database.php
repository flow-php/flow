<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\CreateDatabaseCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\DropDatabaseCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\RunSqlCommand;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\param;
use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services
        ->set('flow.postgresql.command.database_create', CreateDatabaseCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.default_connection')])
        ->tag('console.command');

    $services
        ->set('flow.postgresql.command.database_drop', DropDatabaseCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.default_connection')])
        ->tag('console.command');

    $services
        ->set('flow.postgresql.command.sql_run', RunSqlCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.default_connection')])
        ->tag('console.command');
};
