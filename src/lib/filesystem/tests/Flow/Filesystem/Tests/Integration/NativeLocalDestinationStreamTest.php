<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use Flow\Filesystem\Local\NativeLocalFilesystem;

use function file_get_contents;
use function Flow\Filesystem\DSL\path;
use function fopen;

final class NativeLocalDestinationStreamTest extends NativeLocalFilesystemTestCase
{
    public function test_closing_empty_stream(): void
    {
        $fs = new NativeLocalFilesystem();
        $stream = $fs->writeTo(path(__DIR__ . '/var/file.txt'));
        static::assertTrue($stream->isOpen());
        $stream->close();
        static::assertFalse($stream->isOpen());
    }

    public function test_writing_content_from_resource(): void
    {
        $fs = new NativeLocalFilesystem();

        $resource = fopen(__DIR__ . '/Fixtures/orders.csv', 'rb');
        static::assertIsResource($resource);

        $stream = $fs->writeTo(path(__DIR__ . '/var/orders.csv'));
        $stream->fromResource($resource);
        $stream->close();

        $status = $fs->status(path(__DIR__ . '/var/orders.csv'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame(
            file_get_contents(__DIR__ . '/Fixtures/orders.csv'),
            $fs->readFrom(path(__DIR__ . '/var/orders.csv'))->content(),
        );

        $fs->rm(path(__DIR__ . '/var/orders.csv'));
    }

    public function test_writing_contente(): void
    {
        $fs = new NativeLocalFilesystem();

        $stream = $fs->writeTo(path(__DIR__ . '/var/file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        $status = $fs->status(path(__DIR__ . '/var/file.txt'));
        static::assertNotNull($status);
        static::assertTrue($status->isFile());
        static::assertFalse($status->isDirectory());
        static::assertSame('Hello, World!', $fs->readFrom(path(__DIR__ . '/var/file.txt'))->content());

        $fs->rm(path(__DIR__ . '/var/file.txt'));
    }
}
