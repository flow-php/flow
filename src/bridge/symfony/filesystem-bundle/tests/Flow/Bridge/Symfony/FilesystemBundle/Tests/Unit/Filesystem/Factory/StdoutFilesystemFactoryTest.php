<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem\Factory;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\StdoutFilesystemFactory;
use Flow\Filesystem\Protocol;
use PHPUnit\Framework\TestCase;

final class StdoutFilesystemFactoryTest extends TestCase
{
    public function test_creates_filesystem_with_matching_protocol() : void
    {
        $filesystem = (new StdoutFilesystemFactory())->create(new Protocol('stdout'), []);

        self::assertSame('stdout', $filesystem->protocol()->name);
    }

    public function test_protocol_returns_expected_name() : void
    {
        self::assertSame('stdout', (new StdoutFilesystemFactory())->protocol()->name);
    }

    public function test_throws_on_protocol_mismatch() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem factory for protocol "stdout" cannot create filesystem for protocol "file".');

        (new StdoutFilesystemFactory())->create(new Protocol('file'), []);
    }

    public function test_throws_on_unknown_options() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem factory for protocol "stdout" does not accept any options. Unknown keys: [foo, bar].');

        (new StdoutFilesystemFactory())->create(new Protocol('stdout'), ['foo' => 1, 'bar' => 2]);
    }
}
