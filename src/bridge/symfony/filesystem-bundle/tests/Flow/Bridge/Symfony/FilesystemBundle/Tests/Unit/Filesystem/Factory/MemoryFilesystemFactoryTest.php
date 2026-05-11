<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem\Factory;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\MemoryFilesystemFactory;
use PHPUnit\Framework\TestCase;

final class MemoryFilesystemFactoryTest extends TestCase
{
    public function test_creates_filesystem_with_given_mount_protocol(): void
    {
        static::assertSame('memory', (new MemoryFilesystemFactory())->create('memory', [])->mount()->protocol);
    }

    public function test_throws_on_unknown_options(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Filesystem factory for type "memory" does not accept any options. Unknown keys: [foo, bar].',
        );

        (new MemoryFilesystemFactory())->create('memory', ['foo' => 1, 'bar' => 2]);
    }

    public function test_type_returns_memory(): void
    {
        static::assertSame('memory', (new MemoryFilesystemFactory())->type());
    }
}
