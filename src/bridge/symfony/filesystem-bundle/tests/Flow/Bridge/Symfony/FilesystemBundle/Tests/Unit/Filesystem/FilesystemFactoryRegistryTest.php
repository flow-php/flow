<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\LogicException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\MemoryFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactoryRegistry;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Double\CapturingFilesystemFactory;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Double\StubFilesystemFactory;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\native_local_filesystem;

final class FilesystemFactoryRegistryTest extends TestCase
{
    public function test_accepts_empty_iterable(): void
    {
        $registry = new FilesystemFactoryRegistry([]);

        static::assertSame([], $registry->types());
        static::assertFalse($registry->has('file'));
    }

    public function test_create_mounts_two_filesystems_of_same_type_under_different_protocols(): void
    {
        $registry = new FilesystemFactoryRegistry([new MemoryFilesystemFactory()]);

        $warehouse = $registry->create('default', 'warehouse', ['type' => 'memory']);
        $archive = $registry->create('default', 'archive', ['type' => 'memory']);

        static::assertSame('warehouse', $warehouse->mount()->protocol);
        static::assertSame('archive', $archive->mount()->protocol);
        static::assertNotSame($warehouse, $archive);
    }

    public function test_create_passes_entry_options_without_type_to_factory(): void
    {
        $filesystem = native_local_filesystem('my-fs');
        $capturing = new CapturingFilesystemFactory('file', $filesystem);

        $created = (new FilesystemFactoryRegistry([$capturing]))->create('default', 'my-fs', [
            'type' => 'file',
            'foo' => 'bar',
        ]);

        static::assertSame($filesystem, $created);
        static::assertSame([['mount' => 'my-fs', 'config' => ['foo' => 'bar']]], $capturing->calls);
    }

    public function test_create_throws_when_factory_missing_for_type(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage(
            'Fstab "primary" mount "memory": No filesystem factory registered for type "memory". Available types: [].',
        );

        (new FilesystemFactoryRegistry([]))->create('primary', 'memory', ['type' => 'memory']);
    }

    public function test_create_wraps_factory_error_with_fstab_and_mount(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage(
            'Fstab "primary" mount "memory": Filesystem factory for type "memory" does not accept any options. Unknown keys: [foo].',
        );

        (new FilesystemFactoryRegistry([new MemoryFilesystemFactory()]))->create('primary', 'memory', [
            'type' => 'memory',
            'foo' => 'bar',
        ]);
    }

    public function test_get_throws_on_unknown_type(): void
    {
        $registry = new FilesystemFactoryRegistry([new StubFilesystemFactory('memory')]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No filesystem factory registered for type "file". Available types: [memory].');

        $registry->get('file');
    }

    public function test_has_returns_false_for_unknown_type(): void
    {
        static::assertFalse((new FilesystemFactoryRegistry([new StubFilesystemFactory('memory')]))->has('file'));
    }

    public function test_has_returns_true_for_registered_type(): void
    {
        static::assertTrue((new FilesystemFactoryRegistry([new StubFilesystemFactory('memory')]))->has('memory'));
    }

    public function test_returns_factory_by_type(): void
    {
        $memory = new StubFilesystemFactory('memory');
        $local = new StubFilesystemFactory('file');

        $registry = new FilesystemFactoryRegistry([$memory, $local]);

        static::assertSame($memory, $registry->get('memory'));
        static::assertSame($local, $registry->get('file'));
    }

    public function test_throws_on_duplicate_factory_type(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Duplicate filesystem factory for type "memory".');

        new FilesystemFactoryRegistry([
            new StubFilesystemFactory('memory'),
            new StubFilesystemFactory('memory'),
        ]);
    }

    public function test_types_lists_registered_factory_types(): void
    {
        $registry = new FilesystemFactoryRegistry([
            new StubFilesystemFactory('memory'),
            new StubFilesystemFactory('file'),
        ]);

        static::assertSame(['memory', 'file'], $registry->types());
    }
}
