<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\FilesystemBundle\Command\CatCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\CpCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\FstabResolver;
use Flow\Bridge\Symfony\FilesystemBundle\Command\LsCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\MvCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\RmCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\StatCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Command\TouchCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\AsyncAwsS3FilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\AzureBlobFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\MemoryFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\NativeLocalFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\StdoutFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactoryRegistry;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3Filesystem;
use Flow\Filesystem\Bridge\Azure\AzureBlobFilesystem;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;

use function Symfony\Component\DependencyInjection\Loader\Configurator\service;

return static function (ContainerConfigurator $container): void {
    $services = $container->services();

    $services->set(
        '.flow.filesystem.factory.file',
        NativeLocalFilesystemFactory::class,
    )->private()->tag('flow.filesystem.factory', ['type' => 'file']);

    $services->set(
        '.flow.filesystem.factory.memory',
        MemoryFilesystemFactory::class,
    )->private()->tag('flow.filesystem.factory', ['type' => 'memory']);

    $services->set(
        '.flow.filesystem.factory.stdout',
        StdoutFilesystemFactory::class,
    )->private()->tag('flow.filesystem.factory', ['type' => 'stdout']);

    if (\class_exists(AsyncAWSS3Filesystem::class)) {
        $services->set(
            '.flow.filesystem.factory.aws_s3',
            AsyncAwsS3FilesystemFactory::class,
        )->private()->tag('flow.filesystem.factory', ['type' => 'aws_s3']);
    }

    if (\class_exists(AzureBlobFilesystem::class)) {
        $services->set(
            '.flow.filesystem.factory.azure_blob',
            AzureBlobFilesystemFactory::class,
        )->private()->tag('flow.filesystem.factory', ['type' => 'azure_blob']);
    }

    $services->set('.flow.filesystem.factory_registry', FilesystemFactoryRegistry::class)->private()->args([[]]);

    $services
        ->set('.flow.filesystem.command.fstab_resolver', FstabResolver::class)
        ->private()
        ->args([service('flow.filesystem.fstab_locator'), '%flow.filesystem.default_fstab%']);

    foreach ([
        '.flow.filesystem.command.ls' => LsCommand::class,
        '.flow.filesystem.command.cat' => CatCommand::class,
        '.flow.filesystem.command.cp' => CpCommand::class,
        '.flow.filesystem.command.mv' => MvCommand::class,
        '.flow.filesystem.command.rm' => RmCommand::class,
        '.flow.filesystem.command.stat' => StatCommand::class,
        '.flow.filesystem.command.touch' => TouchCommand::class,
    ] as $serviceId => $commandClass) {
        $services
            ->set($serviceId, $commandClass)
            ->private()
            ->args([service('.flow.filesystem.command.fstab_resolver')])
            ->tag('console.command');
    }
};
