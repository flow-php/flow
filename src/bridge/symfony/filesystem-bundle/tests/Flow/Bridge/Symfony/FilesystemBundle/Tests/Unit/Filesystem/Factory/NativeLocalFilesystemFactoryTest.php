<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem\Factory;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\NativeLocalFilesystemFactory;
use Flow\Filesystem\Protocol;
use PHPUnit\Framework\TestCase;

final class NativeLocalFilesystemFactoryTest extends TestCase
{
    public function test_creates_filesystem_with_matching_protocol() : void
    {
        $filesystem = (new NativeLocalFilesystemFactory())->create(new Protocol('file'), []);

        self::assertSame('file', $filesystem->protocol()->name);
    }

    public function test_protocol_returns_expected_name() : void
    {
        self::assertSame('file', (new NativeLocalFilesystemFactory())->protocol()->name);
    }

    public function test_throws_on_protocol_mismatch() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem factory for protocol "file" cannot create filesystem for protocol "memory".');

        (new NativeLocalFilesystemFactory())->create(new Protocol('memory'), []);
    }

    public function test_throws_on_unknown_options() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem factory for protocol "file" does not accept any options. Unknown keys: [foo, bar].');

        (new NativeLocalFilesystemFactory())->create(new Protocol('file'), ['foo' => 1, 'bar' => 2]);
    }
}
