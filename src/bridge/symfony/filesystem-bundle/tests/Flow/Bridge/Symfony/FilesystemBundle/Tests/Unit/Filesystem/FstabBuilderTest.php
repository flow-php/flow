<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\MemoryFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\NativeLocalFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactoryRegistry;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FstabBuilder;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Double\CapturingFilesystemFactory;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\native_local_filesystem;

final class FstabBuilderTest extends TestCase
{
    public function test_builds_empty_table_when_no_filesystems_provided(): void
    {
        $table = FstabBuilder::build(new FilesystemFactoryRegistry([]), 'default', []);

        static::assertSame([], $table->filesystems());
    }

    public function test_mounts_filesystem_from_factory(): void
    {
        $table = FstabBuilder::build(
            new FilesystemFactoryRegistry([new MemoryFilesystemFactory()]),
            'default',
            ['memory' => ['type' => 'memory']],
        );

        static::assertSame('memory', $table->for('memory')->mount()->protocol);
    }

    public function test_mounts_multiple_filesystems(): void
    {
        $table = FstabBuilder::build(
            new FilesystemFactoryRegistry([
                new NativeLocalFilesystemFactory(),
                new MemoryFilesystemFactory(),
            ]),
            'default',
            [
                'file' => ['type' => 'file'],
                'memory' => ['type' => 'memory'],
            ],
        );

        static::assertSame('file', $table->for('file')->mount()->protocol);
        static::assertSame('memory', $table->for('memory')->mount()->protocol);
    }

    public function test_mounts_two_filesystems_of_same_type_under_different_protocols(): void
    {
        $table = FstabBuilder::build(new FilesystemFactoryRegistry([new MemoryFilesystemFactory()]), 'default', [
            'warehouse' => ['type' => 'memory'],
            'archive' => ['type' => 'memory'],
        ]);

        static::assertSame('warehouse', $table->for('warehouse')->mount()->protocol);
        static::assertSame('archive', $table->for('archive')->mount()->protocol);
        static::assertNotSame($table->for('warehouse'), $table->for('archive'));
    }

    public function test_passes_entry_options_to_factory(): void
    {
        $capturing = new CapturingFilesystemFactory('file', native_local_filesystem('my-fs'));

        FstabBuilder::build(new FilesystemFactoryRegistry([$capturing]), 'default', [
            'my-fs' => ['type' => 'file', 'foo' => 'bar'],
        ]);

        static::assertSame([['mount' => 'my-fs', 'config' => ['foo' => 'bar']]], $capturing->calls);
    }

    public function test_throws_when_factory_missing_for_type(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage(
            'Fstab "primary" mount "memory": No filesystem factory registered for type "memory"',
        );

        FstabBuilder::build(new FilesystemFactoryRegistry([]), 'primary', ['memory' => ['type' => 'memory']]);
    }
}
