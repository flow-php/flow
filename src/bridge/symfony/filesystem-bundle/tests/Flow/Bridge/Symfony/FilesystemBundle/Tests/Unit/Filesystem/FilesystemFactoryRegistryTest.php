<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\{InvalidArgumentException, LogicException};
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactoryRegistry;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Double\StubFilesystemFactory;
use PHPUnit\Framework\TestCase;

final class FilesystemFactoryRegistryTest extends TestCase
{
    public function test_accepts_empty_iterable() : void
    {
        $registry = new FilesystemFactoryRegistry([]);

        self::assertSame([], $registry->types());
        self::assertFalse($registry->has('file'));
    }

    public function test_get_throws_on_unknown_type() : void
    {
        $registry = new FilesystemFactoryRegistry([new StubFilesystemFactory('memory')]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No filesystem factory registered for type "file". Available types: [memory].');

        $registry->get('file');
    }

    public function test_has_returns_false_for_unknown_type() : void
    {
        self::assertFalse((new FilesystemFactoryRegistry([new StubFilesystemFactory('memory')]))->has('file'));
    }

    public function test_has_returns_true_for_registered_type() : void
    {
        self::assertTrue((new FilesystemFactoryRegistry([new StubFilesystemFactory('memory')]))->has('memory'));
    }

    public function test_returns_factory_by_type() : void
    {
        $memory = new StubFilesystemFactory('memory');
        $local = new StubFilesystemFactory('file');

        $registry = new FilesystemFactoryRegistry([$memory, $local]);

        self::assertSame($memory, $registry->get('memory'));
        self::assertSame($local, $registry->get('file'));
    }

    public function test_throws_on_duplicate_factory_type() : void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Duplicate filesystem factory for type "memory".');

        new FilesystemFactoryRegistry([
            new StubFilesystemFactory('memory'),
            new StubFilesystemFactory('memory'),
        ]);
    }

    public function test_types_lists_registered_factory_types() : void
    {
        $registry = new FilesystemFactoryRegistry([
            new StubFilesystemFactory('memory'),
            new StubFilesystemFactory('file'),
        ]);

        self::assertSame(['memory', 'file'], $registry->types());
    }
}
