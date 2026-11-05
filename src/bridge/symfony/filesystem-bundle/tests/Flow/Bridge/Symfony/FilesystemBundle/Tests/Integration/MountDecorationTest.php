<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Integration;

use Flow\Bridge\Symfony\FilesystemBundle\Tests\Fixtures\AliasFilesystemConsumer;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Telemetry\TraceableFilesystem;
use Flow\Filesystem\Testing\FilesystemOperation;
use Flow\Filesystem\Testing\InterceptedFilesystem;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\DependencyInjection\Exception\ServiceNotFoundException;
use Symfony\Component\DependencyInjection\Reference;

use function Flow\Filesystem\DSL\path;

final class MountDecorationTest extends KernelTestCase
{
    public function test_decorators_apply_in_decoration_priority_order(): void
    {
        $this->symfonyContext()->bootTracedMemoryFstab(static function (ContainerBuilder $container): void {
            $container
                ->register('test.priority_10', InterceptedFilesystem::class)
                ->setDecoratedService('flow.filesystem.mount.default.memory', null, 10)
                ->setArguments([new Reference('.inner')])
                ->setPublic(true);
            $container
                ->register('test.priority_0', InterceptedFilesystem::class)
                ->setDecoratedService('flow.filesystem.mount.default.memory', null, 0)
                ->setArguments([new Reference('.inner')])
                ->setPublic(true);
        });

        $order = [];
        $this
            ->symfonyContext()
            ->getService('test.priority_10', InterceptedFilesystem::class)
            ->onCall(static function () use (&$order): void {
                $order[] = 'priority 10';
            });
        $this
            ->symfonyContext()
            ->getService('test.priority_0', InterceptedFilesystem::class)
            ->onCall(static function () use (&$order): void {
                $order[] = 'priority 0';
            });

        $this
            ->symfonyContext()
            ->getService(FilesystemTable::class, FilesystemTable::class)
            ->for('memory')
            ->rm(path('memory://a.txt'));

        static::assertSame(['priority 0', 'priority 10'], $order);
    }

    public function test_injected_mount_alias_is_traceable_around_intercepted_around_inner(): void
    {
        $this->symfonyContext()->bootTracedMemoryFstab(static function (ContainerBuilder $container): void {
            $container
                ->register(InterceptedFilesystem::class, InterceptedFilesystem::class)
                ->setDecoratedService('flow.filesystem.mount.default.memory')
                ->setArguments([new Reference('.inner')])
                ->setPublic(true);
            $container
                ->register('test.alias_consumer', AliasFilesystemConsumer::class)
                ->setAutowired(true)
                ->setPublic(true);
        });

        $exception = new RuntimeException('S3 down');
        $removals = 0;
        $this
            ->symfonyContext()
            ->getService(InterceptedFilesystem::class, InterceptedFilesystem::class)
            ->onCall(static function (FilesystemOperation $operation) use ($exception, &$removals): void {
                if ($operation === FilesystemOperation::Rm && ++$removals === 1) {
                    throw $exception;
                }
            });
        $resolved = $this->symfonyContext()->getService('test.alias_consumer', AliasFilesystemConsumer::class)->memory;
        $resolved->writeTo(path('memory://a.txt'))->append('a')->close();

        static::assertInstanceOf(TraceableFilesystem::class, $resolved);

        try {
            $resolved->rm(path('memory://a.txt'));
            static::fail('The armed rm() was expected to throw');
        } catch (RuntimeException $thrown) {
            static::assertSame($exception, $thrown);
        }

        $resolved->rm(path('memory://a.txt'));

        static::assertNull($resolved->status(path('memory://a.txt')));
    }

    public function test_table_mount_is_traceable_around_intercepted_around_inner(): void
    {
        $this->symfonyContext()->bootTracedMemoryFstab(static function (ContainerBuilder $container): void {
            $container
                ->register(InterceptedFilesystem::class, InterceptedFilesystem::class)
                ->setDecoratedService('flow.filesystem.mount.default.memory')
                ->setArguments([new Reference('.inner')])
                ->setPublic(true);
        });

        $exception = new RuntimeException('S3 down');
        $removals = 0;
        $this
            ->symfonyContext()
            ->getService(InterceptedFilesystem::class, InterceptedFilesystem::class)
            ->onCall(static function (FilesystemOperation $operation) use ($exception, &$removals): void {
                if ($operation === FilesystemOperation::Rm && ++$removals === 1) {
                    throw $exception;
                }
            });
        $resolved = $this->symfonyContext()->getService(FilesystemTable::class, FilesystemTable::class)->for('memory');
        $resolved->writeTo(path('memory://a.txt'))->append('a')->close();

        static::assertInstanceOf(TraceableFilesystem::class, $resolved);

        try {
            $resolved->rm(path('memory://a.txt'));
            static::fail('The armed rm() was expected to throw');
        } catch (RuntimeException $thrown) {
            static::assertSame($exception, $thrown);
        }

        $resolved->rm(path('memory://a.txt'));

        static::assertNull($resolved->status(path('memory://a.txt')));
    }

    public function test_unknown_mount_fails_container_compile(): void
    {
        $this->expectException(ServiceNotFoundException::class);
        $this->expectExceptionMessage('flow.filesystem.mount.default.typo');

        $this->symfonyContext()->bootTracedMemoryFstab(static function (ContainerBuilder $container): void {
            $container
                ->register(InterceptedFilesystem::class, InterceptedFilesystem::class)
                ->setDecoratedService('flow.filesystem.mount.default.typo')
                ->setArguments([new Reference('.inner')])
                ->setPublic(true);
        });
    }

    public function test_without_decorators_table_mounts_the_factory_filesystem(): void
    {
        $this->symfonyContext()->bootTracedMemoryFstab(static function (ContainerBuilder $container): void {});

        $resolved = $this->symfonyContext()->getService(FilesystemTable::class, FilesystemTable::class)->for('memory');
        $resolved->writeTo(path('memory://a.txt'))->append('a')->close();

        static::assertInstanceOf(TraceableFilesystem::class, $resolved);
        static::assertSame('a', $resolved->readFrom(path('memory://a.txt'))->content());
    }
}
