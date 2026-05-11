<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\CurrentCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\DiffCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\ExecuteCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\GenerateCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\LatestCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\ListCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\MigrateCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\StatusCommand;
use Flow\Bridge\Symfony\PostgreSqlBundle\Command\UpToDateCommand;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\param;
use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services
        ->set('flow.postgresql.command.current', CurrentCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.migrations.default_connection')])
        ->tag('console.command');

    $services
        ->set('flow.postgresql.command.latest', LatestCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.migrations.default_connection')])
        ->tag('console.command');

    $services
        ->set('flow.postgresql.command.status', StatusCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.migrations.default_connection')])
        ->tag('console.command');

    $services
        ->set('flow.postgresql.command.list', ListCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.migrations.default_connection')])
        ->tag('console.command');

    $services
        ->set('flow.postgresql.command.migrate', MigrateCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.migrations.default_connection')])
        ->tag('console.command');

    $services
        ->set('flow.postgresql.command.execute', ExecuteCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.migrations.default_connection')])
        ->tag('console.command');

    $services
        ->set('flow.postgresql.command.diff', DiffCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.migrations.default_connection')])
        ->tag('console.command');

    $services
        ->set('flow.postgresql.command.generate', GenerateCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.migrations.default_connection')])
        ->tag('console.command');

    $services
        ->set('flow.postgresql.command.up_to_date', UpToDateCommand::class)
        ->args([service('flow.postgresql.command_locator'), param('flow.postgresql.migrations.default_connection')])
        ->tag('console.command');
};
