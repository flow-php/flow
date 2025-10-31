<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

final class NativeLocalDestinationStreamTest extends NativeLocalFilesystemTestCase
{
    public function test_closing_empty_stream() : void
    {
        $fs = new NativeLocalFilesystem();
        $stream = $fs->writeTo(Path::from(__DIR__ . '/var/file.txt'));
        self::assertTrue($stream->isOpen());
        $stream->close();
        self::assertFalse($stream->isOpen());
    }

    public function test_writing_content_from_resource() : void
    {
        $fs = new NativeLocalFilesystem();

        $stream = $fs->writeTo(Path::from(__DIR__ . '/var/orders.csv'));
        $stream->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $stream->close();

        self::assertTrue($fs->status(Path::from(__DIR__ . '/var/orders.csv'))->isFile());
        self::assertFalse($fs->status(Path::from(__DIR__ . '/var/orders.csv'))->isDirectory());
        self::assertSame(\file_get_contents(__DIR__ . '/Fixtures/orders.csv'), $fs->readFrom(Path::from(__DIR__ . '/var/orders.csv'))->content());

        $fs->rm(Path::from(__DIR__ . '/var/orders.csv'));
    }

    public function test_writing_contente() : void
    {
        $fs = new NativeLocalFilesystem();

        $stream = $fs->writeTo(Path::from(__DIR__ . '/var/file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        self::assertTrue($fs->status(Path::from(__DIR__ . '/var/file.txt'))->isFile());
        self::assertFalse($fs->status(Path::from(__DIR__ . '/var/file.txt'))->isDirectory());
        self::assertSame('Hello, World!', $fs->readFrom(Path::from(__DIR__ . '/var/file.txt'))->content());

        $fs->rm(Path::from(__DIR__ . '/var/file.txt'));
    }
}
