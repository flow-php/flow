<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Integration;

use Flow\Filesystem\Exception\FileExistsException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;

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

    public function test_exclusive_stream_links_the_temporary_file_at_close(): void
    {
        $stream = NativeLocalDestinationStream::openExclusive(
            path(__DIR__ . '/var/exclusive.txt'),
            path(__DIR__ . '/var/.tmp.exclusive.txt'),
        );
        $stream->append('whole');

        static::assertSame(__DIR__ . '/var/exclusive.txt', $stream->path()->path());
        static::assertFileDoesNotExist(__DIR__ . '/var/exclusive.txt');

        $stream->close();
        $stream->close();

        static::assertSame('whole', file_get_contents(__DIR__ . '/var/exclusive.txt'));
        static::assertFileDoesNotExist(__DIR__ . '/var/.tmp.exclusive.txt');
    }

    public function test_exclusive_stream_refuses_an_existing_target_and_removes_the_temporary_file(): void
    {
        (new NativeLocalFilesystem())
            ->writeTo(path(__DIR__ . '/var/exclusive.txt'))
            ->append('first')
            ->close();
        $stream = NativeLocalDestinationStream::openExclusive(
            path(__DIR__ . '/var/exclusive.txt'),
            path(__DIR__ . '/var/.tmp.exclusive.txt'),
        );
        $stream->append('second');

        try {
            $stream->close();
            static::fail('Expected the existing target to be refused');
        } catch (FileExistsException) {
        }

        $stream->close();

        static::assertSame('first', file_get_contents(__DIR__ . '/var/exclusive.txt'));
        static::assertFileDoesNotExist(__DIR__ . '/var/.tmp.exclusive.txt');
    }

    public function test_exclusive_stream_without_a_target_directory_fails_and_removes_the_temporary_file(): void
    {
        $stream = NativeLocalDestinationStream::openExclusive(
            path(__DIR__ . '/var/missing/exclusive.txt'),
            path(__DIR__ . '/var/.tmp.exclusive.txt'),
        );

        try {
            $stream->close();
            static::fail('Expected the link into a missing directory to fail');
        } catch (RuntimeException $e) {
            static::assertStringContainsString('was not created', $e->getMessage());
        }

        static::assertFileDoesNotExist(__DIR__ . '/var/.tmp.exclusive.txt');
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
