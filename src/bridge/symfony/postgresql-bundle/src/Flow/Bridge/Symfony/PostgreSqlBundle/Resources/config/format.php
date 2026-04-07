<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\FormatSqlCommand;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

return static function (ContainerConfigurator $container) : void {
    $services = $container->services();

    $services->set('flow.postgresql.command.sql_format', FormatSqlCommand::class)
        ->tag('console.command');
};
