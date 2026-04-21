<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\BuildFstabsPass;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FstabBuilder;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\BuildFstabsPassContext;
use Flow\Filesystem\FilesystemTable;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\{ContainerBuilder, Reference};

final class BuildFstabsPassTest extends TestCase
{
    private BuildFstabsPassContext $context;

    protected function setUp() : void
    {
        $this->context = new BuildFstabsPassContext();
    }

    public function test_camel_cases_snake_case_fstab_names_in_alias() : void
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

        self::assertTrue($container->hasAlias(FilesystemTable::class . ' $myWarehouseFstab'));
    }

    public function test_does_not_register_fqcn_alias_when_default_fstab_null() : void
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

        self::assertFalse($container->hasAlias(FilesystemTable::class));
    }

    public function test_does_nothing_when_config_parameter_missing() : void
    {
        $container = new ContainerBuilder();

        (new BuildFstabsPass())->process($container);

        self::assertFalse($container->hasDefinition('.flow_filesystem.fstab.default'));
    }

    public function test_passes_entries_as_third_argument() : void
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

        $arguments = $container->getDefinition('.flow_filesystem.fstab.default')->getArguments();
        self::assertSame(['memory' => ['type' => 'memory', 'foo' => 'bar']], $arguments[2]);
    }

    public function test_passes_registry_reference_and_fstab_name_as_first_arguments() : void
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

        $arguments = $container->getDefinition('.flow_filesystem.fstab.default')->getArguments();
        self::assertInstanceOf(Reference::class, $arguments[0]);
        self::assertSame('.flow_filesystem.factory_registry', (string) $arguments[0]);
        self::assertSame('default', $arguments[1]);
    }

    public function test_registers_fqcn_alias_pointing_to_default_fstab() : void
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

        self::assertTrue($container->hasAlias(FilesystemTable::class));
        $alias = $container->getAlias(FilesystemTable::class);
        self::assertSame('.flow_filesystem.fstab.secondary', (string) $alias);
        self::assertTrue($alias->isPublic());
    }

    public function test_registers_named_argument_alias_for_each_fstab() : void
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
        self::assertTrue($container->hasAlias($aliasId));
        $alias = $container->getAlias($aliasId);
        self::assertSame('.flow_filesystem.fstab.default', (string) $alias);
        self::assertTrue($alias->isPublic());
    }

    public function test_registers_private_fstab_service_for_each_configured_fstab() : void
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

        $defaultDef = $container->getDefinition('.flow_filesystem.fstab.default');
        self::assertSame(FilesystemTable::class, $defaultDef->getClass());
        self::assertFalse($defaultDef->isPublic());
        self::assertSame([FstabBuilder::class, 'build'], $defaultDef->getFactory());

        self::assertTrue($container->hasDefinition('.flow_filesystem.fstab.secondary'));
        self::assertFalse($container->getDefinition('.flow_filesystem.fstab.secondary')->isPublic());
    }

    public function test_throws_on_unknown_default_fstab() : void
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

    public function test_throws_on_unknown_type_at_compile_time() : void
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
        $this->expectExceptionMessage('Fstab "default" mount "ftp": no filesystem factory registered for type "aws_s3"');

        (new BuildFstabsPass())->process($container);
    }
}
