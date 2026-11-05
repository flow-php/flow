<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\BuildFstabsPass;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\AsyncAwsS3FilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\AzureBlobFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\BuildFstabsPassContext;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\FilesystemTable;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

final class BuildFstabsPassTest extends TestCase
{
    private BuildFstabsPassContext $context;

    protected function setUp(): void
    {
        $this->context = new BuildFstabsPassContext();
    }

    public function test_camel_cases_snake_case_fstab_names_in_alias(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'my_warehouse',
            'fstabs' => [
                'my_warehouse' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        static::assertTrue($container->hasAlias(FilesystemTable::class . ' $myWarehouseFstab'));
    }

    public function test_does_not_register_fqcn_alias_when_default_fstab_null(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => null,
            'fstabs' => [
                'primary' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        static::assertFalse($container->hasAlias(FilesystemTable::class));
    }

    public function test_does_nothing_when_config_parameter_missing(): void
    {
        $container = new ContainerBuilder();

        (new BuildFstabsPass())->process($container);

        static::assertFalse($container->hasDefinition('.flow.filesystem.fstab.default'));
    }

    public function test_fstab_service_is_built_from_its_mount_services(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        $definition = $container->getDefinition('.flow.filesystem.fstab.default');

        static::assertNull($definition->getFactory());
        static::assertEquals(
            [
                new Reference('flow.filesystem.mount.default.memory'),
                new Reference('flow.filesystem.mount.default.file'),
            ],
            $definition->getArguments(),
        );
    }

    public function test_registers_private_mount_service_for_each_mount(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory', 'foo' => 'bar'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        $definition = $container->getDefinition('flow.filesystem.mount.default.memory');

        static::assertSame(Filesystem::class, $definition->getClass());
        static::assertEquals([new Reference('.flow.filesystem.factory_registry'), 'create'], $definition->getFactory());
        static::assertSame(['default', 'memory', ['type' => 'memory', 'foo' => 'bar']], $definition->getArguments());
        static::assertFalse($definition->isPublic());
    }

    public function test_registers_fqcn_alias_pointing_to_default_fstab(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'secondary',
            'fstabs' => [
                'primary' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
                'secondary' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        static::assertTrue($container->hasAlias(FilesystemTable::class));
        $alias = $container->getAlias(FilesystemTable::class);
        static::assertSame('.flow.filesystem.fstab.secondary', (string) $alias);
        static::assertTrue($alias->isPublic());
    }

    public function test_registers_named_argument_alias_for_each_fstab(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        $aliasId = FilesystemTable::class . ' $defaultFstab';
        static::assertTrue($container->hasAlias($aliasId));
        $alias = $container->getAlias($aliasId);
        static::assertSame('.flow.filesystem.fstab.default', (string) $alias);
        static::assertTrue($alias->isPublic());
    }

    public function test_registers_private_fstab_service_for_each_configured_fstab(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
                'secondary' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        $defaultDef = $container->getDefinition('.flow.filesystem.fstab.default');
        static::assertSame(FilesystemTable::class, $defaultDef->getClass());
        static::assertFalse($defaultDef->isPublic());

        static::assertTrue($container->hasDefinition('.flow.filesystem.fstab.secondary'));
        static::assertFalse($container->getDefinition('.flow.filesystem.fstab.secondary')->isPublic());
    }

    public function test_throws_on_unknown_default_fstab(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'nope',
            'fstabs' => [
                'primary' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('flow_filesystem: default_fstab "nope"');

        (new BuildFstabsPass())->process($container);
    }

    public function test_throws_on_unknown_type_at_compile_time(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'ftp' => ['type' => 'aws_s3'],
                    ],
                ],
            ],
        ]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage(
            'Fstab "default" mount "ftp": no filesystem factory registered for type "aws_s3"',
        );

        (new BuildFstabsPass())->process($container);
    }

    public function test_registers_per_mount_filesystem_service_resolving_through_fstab(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        $definition = $container->getDefinition('.flow.filesystem.fs.default.memory');

        static::assertSame(Filesystem::class, $definition->getClass());
        static::assertEquals([new Reference('.flow.filesystem.fstab.default'), 'for'], $definition->getFactory());
        static::assertSame(['memory'], $definition->getArguments());
        static::assertFalse($definition->isPublic());
    }

    public function test_registers_bare_and_prefixed_mount_aliases_for_default_fstab(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        static::assertSame(
            '.flow.filesystem.fs.default.memory',
            (string) $container->getAlias(Filesystem::class . ' $memory'),
        );
        static::assertSame(
            '.flow.filesystem.fs.default.memory',
            (string) $container->getAlias(Filesystem::class . ' $defaultMemory'),
        );
    }

    public function test_registers_only_prefixed_mount_alias_for_non_default_fstab(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'primary',
            'fstabs' => [
                'primary' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                    ],
                ],
                'secondary' => [
                    'filesystems' => [
                        'file' => ['type' => 'file'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        static::assertTrue($container->hasAlias(Filesystem::class . ' $secondaryFile'));
        static::assertFalse($container->hasAlias(Filesystem::class . ' $file'));
    }

    public function test_camel_cases_separator_protocols_in_mount_aliases(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'aws-s3' => ['type' => 'memory'],
                    ],
                ],
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        static::assertTrue($container->hasDefinition('.flow.filesystem.fs.default.aws-s3'));
        static::assertTrue($container->hasAlias(Filesystem::class . ' $awsS3'));
        static::assertTrue($container->hasAlias(Filesystem::class . ' $defaultAwsS3'));
    }

    public function test_resolves_aws_s3_client_service_ids_to_references(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        's3' => [
                            'type' => 'aws_s3',
                            'bucket' => 'reports',
                            'client' => [
                                'region' => 'eu-west-1',
                                'http_client_service_id' => 'app.http_client',
                                'logger_service_id' => 'app.logger',
                            ],
                        ],
                    ],
                ],
            ],
        ]);
        $container->register(
            '.flow.filesystem.factory.aws_s3',
            AsyncAwsS3FilesystemFactory::class,
        )->addTag('flow.filesystem.factory', ['type' => 'aws_s3']);

        (new BuildFstabsPass())->process($container);

        static::assertEquals(
            [
                'default',
                's3',
                [
                    'type' => 'aws_s3',
                    'bucket' => 'reports',
                    'client' => [
                        'region' => 'eu-west-1',
                        'http_client' => new Reference('app.http_client'),
                        'logger' => new Reference('app.logger'),
                    ],
                ],
            ],
            $container->getDefinition('flow.filesystem.mount.default.s3')->getArguments(),
        );
    }

    public function test_resolves_azure_blob_client_service_ids_to_references(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'azure' => [
                            'type' => 'azure_blob',
                            'container' => 'reports',
                            'client' => [
                                'account_name' => 'flow',
                                'http_client_service' => 'app.http_client',
                                'request_factory_service' => 'app.request_factory',
                                'stream_factory_service' => 'app.stream_factory',
                                'logger_service_id' => 'app.logger',
                            ],
                        ],
                    ],
                ],
            ],
        ]);
        $container->register(
            '.flow.filesystem.factory.azure_blob',
            AzureBlobFilesystemFactory::class,
        )->addTag('flow.filesystem.factory', ['type' => 'azure_blob']);

        (new BuildFstabsPass())->process($container);

        static::assertEquals(
            [
                'default',
                'azure',
                [
                    'type' => 'azure_blob',
                    'container' => 'reports',
                    'client' => [
                        'account_name' => 'flow',
                        'http_client' => new Reference('app.http_client'),
                        'request_factory' => new Reference('app.request_factory'),
                        'stream_factory' => new Reference('app.stream_factory'),
                        'logger' => new Reference('app.logger'),
                    ],
                ],
            ],
            $container->getDefinition('flow.filesystem.mount.default.azure')->getArguments(),
        );
    }

    public function test_skips_a_config_parameter_that_is_not_an_array(): void
    {
        $container = new ContainerBuilder();
        $container->setParameter(BuildFstabsPass::CONFIG_PARAMETER, 'not an array');

        (new BuildFstabsPass())->process($container);

        static::assertFalse($container->hasAlias(FilesystemTable::class));
    }

    public function test_skips_fstabs_and_mounts_that_are_not_arrays(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'default',
            'fstabs' => [
                'default' => [
                    'filesystems' => [
                        'memory' => ['type' => 'memory'],
                        'broken' => 'not an array',
                    ],
                ],
                'invalid' => 'not an array',
            ],
        ]);

        (new BuildFstabsPass())->process($container);

        static::assertTrue($container->hasDefinition('flow.filesystem.mount.default.memory'));
        static::assertFalse($container->hasDefinition('flow.filesystem.mount.default.broken'));
        static::assertFalse($container->hasDefinition('.flow.filesystem.fstab.invalid'));
    }
}
