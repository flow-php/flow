<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\{InvalidArgumentException, LogicException};
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactoryRegistry;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Double\StubFilesystemFactory;
use Flow\Filesystem\Protocol;
use PHPUnit\Framework\TestCase;

final class FilesystemFactoryRegistryTest extends TestCase
{
    public function test_accepts_empty_iterable() : void
    {
        $registry = new FilesystemFactoryRegistry([]);

        self::assertSame([], $registry->protocols());
        self::assertFalse($registry->has(new Protocol('file')));
    }

    public function test_get_throws_on_unknown_protocol() : void
    {
        $registry = new FilesystemFactoryRegistry([new StubFilesystemFactory('memory')]);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('No filesystem factory registered for protocol "file". Available protocols: [memory].');

        $registry->get(new Protocol('file'));
    }

    public function test_has_returns_false_for_unknown_protocol() : void
    {
        $registry = new FilesystemFactoryRegistry([new StubFilesystemFactory('memory')]);

        self::assertFalse($registry->has(new Protocol('file')));
    }

    public function test_has_returns_true_for_registered_protocol() : void
    {
        $registry = new FilesystemFactoryRegistry([new StubFilesystemFactory('memory')]);

        self::assertTrue($registry->has(new Protocol('memory')));
    }

    public function test_protocols_lists_registered_factory_protocols() : void
    {
        $registry = new FilesystemFactoryRegistry([
            new StubFilesystemFactory('memory'),
            new StubFilesystemFactory('file'),
        ]);

        self::assertSame(['memory', 'file'], $registry->protocols());
    }

    public function test_returns_factory_by_protocol() : void
    {
        $memory = new StubFilesystemFactory('memory');
        $local = new StubFilesystemFactory('file');

        $registry = new FilesystemFactoryRegistry([$memory, $local]);

        self::assertSame($memory, $registry->get(new Protocol('memory')));
        self::assertSame($local, $registry->get(new Protocol('file')));
    }

    public function test_throws_on_duplicate_factory_protocol() : void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Duplicate filesystem factory for protocol "memory".');

        new FilesystemFactoryRegistry([
            new StubFilesystemFactory('memory'),
            new StubFilesystemFactory('memory'),
        ]);
    }
}
