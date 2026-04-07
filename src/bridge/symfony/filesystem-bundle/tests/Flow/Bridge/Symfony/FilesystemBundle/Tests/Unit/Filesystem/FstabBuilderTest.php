<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem;

use function Flow\Filesystem\DSL\native_local_filesystem;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\{MemoryFilesystemFactory, NativeLocalFilesystemFactory};
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\{FilesystemFactoryRegistry, FstabBuilder};
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Double\CapturingFilesystemFactory;
use Flow\Filesystem\Protocol;
use PHPUnit\Framework\TestCase;

final class FstabBuilderTest extends TestCase
{
    public function test_builds_empty_table_when_no_filesystems_provided() : void
    {
        $table = FstabBuilder::build(
            new FilesystemFactoryRegistry([]),
            'default',
            [],
        );

        self::assertSame([], $table->filesystems());
    }

    public function test_mounts_filesystem_from_factory() : void
    {
        $table = FstabBuilder::build(
            new FilesystemFactoryRegistry([new MemoryFilesystemFactory()]),
            'default',
            ['memory' => []],
        );

        self::assertSame('memory', $table->for(new Protocol('memory'))->protocol()->name);
    }

    public function test_mounts_multiple_filesystems() : void
    {
        $table = FstabBuilder::build(
            new FilesystemFactoryRegistry([
                new NativeLocalFilesystemFactory(),
                new MemoryFilesystemFactory(),
            ]),
            'default',
            [
                'file' => [],
                'memory' => [],
            ],
        );

        self::assertSame('file', $table->for(new Protocol('file'))->protocol()->name);
        self::assertSame('memory', $table->for(new Protocol('memory'))->protocol()->name);
    }

    public function test_passes_entry_options_to_factory() : void
    {
        $capturing = new CapturingFilesystemFactory('my-fs', native_local_filesystem());

        FstabBuilder::build(
            new FilesystemFactoryRegistry([$capturing]),
            'default',
            [
                'my-fs' => ['foo' => 'bar'],
            ],
        );

        self::assertSame([['protocol' => 'my-fs', 'config' => ['foo' => 'bar']]], $capturing->calls);
    }

    public function test_throws_when_factory_missing_for_protocol() : void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Fstab "primary" protocol "memory": No filesystem factory registered for protocol "memory".');

        FstabBuilder::build(
            new FilesystemFactoryRegistry([]),
            'primary',
            ['memory' => []],
        );
    }

    public function test_throws_when_protocol_name_is_invalid() : void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Fstab "default" protocol "1bad"');

        FstabBuilder::build(
            new FilesystemFactoryRegistry([]),
            'default',
            ['1bad' => []],
        );
    }
}
