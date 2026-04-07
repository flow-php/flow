<?php

declare(strict_types=1);

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;
use Flow\Bridge\Symfony\FilesystemBundle\Command\{CatCommand, CpCommand, FstabResolver, LsCommand, MvCommand, RmCommand, StatCommand, TouchCommand};
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\{AsyncAwsS3FilesystemFactory, AzureBlobFilesystemFactory, MemoryFilesystemFactory, NativeLocalFilesystemFactory, StdoutFilesystemFactory};
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactoryRegistry;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3Filesystem;
use Flow\Filesystem\Bridge\Azure\AzureBlobFilesystem;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

return static function (ContainerConfigurator $container) : void {
    $services = $container->services();

    $services
        ->set('.flow_filesystem.factory.file', NativeLocalFilesystemFactory::class)
        ->private()
        ->tag('flow_filesystem.factory', ['protocol' => 'file']);

    $services
        ->set('.flow_filesystem.factory.memory', MemoryFilesystemFactory::class)
        ->private()
        ->tag('flow_filesystem.factory', ['protocol' => 'memory']);

    $services
        ->set('.flow_filesystem.factory.stdout', StdoutFilesystemFactory::class)
        ->private()
        ->tag('flow_filesystem.factory', ['protocol' => 'stdout']);

    if (\class_exists(AsyncAWSS3Filesystem::class)) {
        $services
            ->set('.flow_filesystem.factory.aws-s3', AsyncAwsS3FilesystemFactory::class)
            ->private()
            ->args([service('service_container')])
            ->tag('flow_filesystem.factory', ['protocol' => 'aws-s3']);
    }

    if (\class_exists(AzureBlobFilesystem::class)) {
        $services
            ->set('.flow_filesystem.factory.azure-blob', AzureBlobFilesystemFactory::class)
            ->private()
            ->args([service('service_container')])
            ->tag('flow_filesystem.factory', ['protocol' => 'azure-blob']);
    }

    $services
        ->set('.flow_filesystem.factory_registry', FilesystemFactoryRegistry::class)
        ->private()
        ->args([[]]);

    $services
        ->set('.flow_filesystem.command.fstab_resolver', FstabResolver::class)
        ->private()
        ->args([service('flow_filesystem.fstab_locator'), '%flow_filesystem.default_fstab%']);

    foreach ([
        '.flow_filesystem.command.ls' => LsCommand::class,
        '.flow_filesystem.command.cat' => CatCommand::class,
        '.flow_filesystem.command.cp' => CpCommand::class,
        '.flow_filesystem.command.mv' => MvCommand::class,
        '.flow_filesystem.command.rm' => RmCommand::class,
        '.flow_filesystem.command.stat' => StatCommand::class,
        '.flow_filesystem.command.touch' => TouchCommand::class,
    ] as $serviceId => $commandClass) {
        $services
            ->set($serviceId, $commandClass)
            ->private()
            ->args([service('.flow_filesystem.command.fstab_resolver')])
            ->tag('console.command');
    }
};
