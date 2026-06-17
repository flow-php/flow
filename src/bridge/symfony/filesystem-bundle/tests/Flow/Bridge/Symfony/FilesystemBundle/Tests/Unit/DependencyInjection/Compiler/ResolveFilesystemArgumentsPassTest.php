<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\DependencyInjection\Compiler;

use Flow\Bridge\Symfony\FilesystemBundle\DependencyInjection\Compiler\ResolveFilesystemArgumentsPass;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\ResolveFilesystemArgumentsPassContext;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Reference;

final class ResolveFilesystemArgumentsPassTest extends TestCase
{
    private ResolveFilesystemArgumentsPassContext $context;

    protected function setUp(): void
    {
        $this->context = new ResolveFilesystemArgumentsPassContext();
    }

    public function test_binds_argument_to_default_fstab_mount_when_fstab_omitted(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'primary',
            'fstabs' => [
                'primary' => ['filesystems' => ['memory' => ['type' => 'memory']]],
            ],
        ]);
        $definition = $this->context->registerConsumer($container, 'app.consumer', [
            'argument' => 'filesystem',
            'mount' => 'memory',
            'fstab' => '',
        ]);

        (new ResolveFilesystemArgumentsPass())->process($container);

        static::assertEquals(
            new Reference('.flow.filesystem.fs.primary.memory'),
            $definition->getArgument('$filesystem'),
        );
    }

    public function test_binds_argument_to_explicit_fstab_mount(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'primary',
            'fstabs' => [
                'primary' => ['filesystems' => ['memory' => ['type' => 'memory']]],
                'archive' => ['filesystems' => ['file' => ['type' => 'file']]],
            ],
        ]);
        $definition = $this->context->registerConsumer($container, 'app.consumer', [
            'argument' => 'coldStorage',
            'mount' => 'file',
            'fstab' => 'archive',
        ]);

        (new ResolveFilesystemArgumentsPass())->process($container);

        static::assertEquals(
            new Reference('.flow.filesystem.fs.archive.file'),
            $definition->getArgument('$coldStorage'),
        );
    }

    public function test_does_nothing_when_config_parameter_missing(): void
    {
        $container = new ContainerBuilder();

        (new ResolveFilesystemArgumentsPass())->process($container);

        static::assertFalse($container->hasDefinition('app.consumer'));
    }

    public function test_throws_when_no_default_fstab_and_fstab_omitted(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => null,
            'fstabs' => [
                'primary' => ['filesystems' => ['memory' => ['type' => 'memory']]],
            ],
        ]);
        $this->context->registerConsumer($container, 'app.consumer', [
            'argument' => 'filesystem',
            'mount' => 'memory',
            'fstab' => '',
        ]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Service "app.consumer" uses #[AsFilesystem(\'memory\')] without an fstab');

        (new ResolveFilesystemArgumentsPass())->process($container);
    }

    public function test_throws_on_unknown_fstab(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'primary',
            'fstabs' => [
                'primary' => ['filesystems' => ['memory' => ['type' => 'memory']]],
            ],
        ]);
        $this->context->registerConsumer($container, 'app.consumer', [
            'argument' => 'filesystem',
            'mount' => 'memory',
            'fstab' => 'nope',
        ]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('references fstab "nope" which is not configured. Available fstabs: [primary]');

        (new ResolveFilesystemArgumentsPass())->process($container);
    }

    public function test_throws_on_unknown_mount(): void
    {
        $container = $this->context->containerWithConfig([
            'default_fstab' => 'primary',
            'fstabs' => [
                'primary' => ['filesystems' => ['memory' => ['type' => 'memory']]],
            ],
        ]);
        $this->context->registerConsumer($container, 'app.consumer', [
            'argument' => 'filesystem',
            'mount' => 'warehouse',
            'fstab' => 'primary',
        ]);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage(
            'references mount "warehouse" which is not mounted in fstab "primary". Available mounts: [memory]',
        );

        (new ResolveFilesystemArgumentsPass())->process($container);
    }
}
