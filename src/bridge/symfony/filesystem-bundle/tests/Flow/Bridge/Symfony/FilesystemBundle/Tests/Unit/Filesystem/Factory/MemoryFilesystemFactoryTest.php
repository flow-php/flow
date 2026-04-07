<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem\Factory;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\MemoryFilesystemFactory;
use Flow\Filesystem\Protocol;
use PHPUnit\Framework\TestCase;

final class MemoryFilesystemFactoryTest extends TestCase
{
    public function test_creates_filesystem_with_matching_protocol() : void
    {
        $filesystem = (new MemoryFilesystemFactory())->create(new Protocol('memory'), []);

        self::assertSame('memory', $filesystem->protocol()->name);
    }

    public function test_protocol_returns_expected_name() : void
    {
        self::assertSame('memory', (new MemoryFilesystemFactory())->protocol()->name);
    }

    public function test_throws_on_protocol_mismatch() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem factory for protocol "memory" cannot create filesystem for protocol "file".');

        (new MemoryFilesystemFactory())->create(new Protocol('file'), []);
    }

    public function test_throws_on_unknown_options() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem factory for protocol "memory" does not accept any options. Unknown keys: [foo, bar].');

        (new MemoryFilesystemFactory())->create(new Protocol('memory'), ['foo' => 1, 'bar' => 2]);
    }
}
