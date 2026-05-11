<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem\Factory;

use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\NativeLocalFilesystemFactory;
use PHPUnit\Framework\TestCase;

final class NativeLocalFilesystemFactoryTest extends TestCase
{
    public function test_creates_filesystem_with_given_mount_protocol(): void
    {
        static::assertSame('file', (new NativeLocalFilesystemFactory())->create('file', [])->mount()->protocol);
    }

    public function test_creates_filesystem_with_non_canonical_mount_protocol(): void
    {
        static::assertSame(
            'project-root',
            (new NativeLocalFilesystemFactory())->create('project-root', [])->mount()->protocol,
        );
    }

    public function test_throws_on_unknown_options(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Filesystem factory for type "file" does not accept any options. Unknown keys: [foo, bar].',
        );

        (new NativeLocalFilesystemFactory())->create('file', ['foo' => 1, 'bar' => 2]);
    }

    public function test_type_returns_file(): void
    {
        static::assertSame('file', (new NativeLocalFilesystemFactory())->type());
    }
}
