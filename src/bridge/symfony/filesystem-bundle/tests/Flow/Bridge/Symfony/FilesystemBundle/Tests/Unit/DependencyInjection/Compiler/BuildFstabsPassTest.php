<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\BuildFstabsPass;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FstabBuilder;
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

    public function test_passes_entries_as_third_argument(): void
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

        $arguments = $container->getDefinition('.flow.filesystem.fstab.default')->getArguments();
        static::assertSame(['memory' => ['type' => 'memory', 'foo' => 'bar']], $arguments[2]);
    }

    public function test_passes_registry_reference_and_fstab_name_as_first_arguments(): void
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

        $arguments = $container->getDefinition('.flow.filesystem.fstab.default')->getArguments();
        static::assertInstanceOf(Reference::class, $arguments[0]);
        static::assertSame('.flow.filesystem.factory_registry', (string) $arguments[0]);
        static::assertSame('default', $arguments[1]);
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
        static::assertSame([FstabBuilder::class, 'build'], $defaultDef->getFactory());

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
}
