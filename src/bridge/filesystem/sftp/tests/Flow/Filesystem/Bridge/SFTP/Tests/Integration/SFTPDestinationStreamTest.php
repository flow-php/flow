<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Integration;

use function fclose;
use function Flow\Filesystem\Bridge\SFTP\DSL\sftp_filesystem_options;
use function Flow\Filesystem\DSL\path;
use function fopen;
use function fwrite;
use function glob;
use function rewind;
use function str_repeat;
use function strlen;
use function substr;
use function sys_get_temp_dir;

final class SFTPDestinationStreamTest extends SFTPTestCase
{
    public function test_appending_to_an_existing_file_keeps_what_was_there(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), "id,name\n1,one\n");

        $stream = $this->sftpContext()->filesystem()->appendTo(path('sftp:///upload/orders.csv'));
        $stream->append("2,two\n");
        $stream->close();

        static::assertSame(
            "id,name\n1,one\n2,two\n",
            $this->sftpContext()->contentOf(path('sftp:///upload/orders.csv')),
        );
    }

    public function test_appending_to_a_missing_file_creates_it(): void
    {
        $stream = $this->sftpContext()->filesystem()->appendTo(path('sftp:///upload/new.csv'));
        $stream->append("id\n1\n");
        $stream->close();

        static::assertSame("id\n1\n", $this->sftpContext()->contentOf(path('sftp:///upload/new.csv')));
    }

    public function test_closing_a_blank_stream_twice_is_a_no_op(): void
    {
        $stream = $this->sftpContext()->filesystem()->writeTo(path('sftp:///upload/empty.csv'));

        static::assertTrue($stream->isOpen());
        $stream->close();
        $stream->close();

        static::assertFalse($stream->isOpen());
        static::assertSame('', $this->sftpContext()->contentOf(path('sftp:///upload/empty.csv')));
    }

    public function test_no_block_files_are_left_behind_after_a_multi_block_write(): void
    {
        $blockFilesBefore = glob(sys_get_temp_dir() . '/*') ?: [];

        $stream = $this
            ->sftpContext()
            ->filesystem(sftp_filesystem_options()->withBlockSize(1024))
            ->writeTo(path('sftp:///upload/blocks.txt'));
        $stream->append(str_repeat('a', 4096));
        $stream->close();

        static::assertSame(4096, $this->sftpContext()->sizeOf(path('sftp:///upload/blocks.txt')));
        static::assertSame($blockFilesBefore, glob(sys_get_temp_dir() . '/*') ?: []);
    }

    public function test_no_block_file_is_left_behind_when_nothing_was_written(): void
    {
        $blockFilesBefore = glob(sys_get_temp_dir() . '/*') ?: [];

        $this->sftpContext()->filesystem()->writeTo(path('sftp:///upload/empty.csv'))->close();

        static::assertSame($blockFilesBefore, glob(sys_get_temp_dir() . '/*') ?: []);
    }

    public function test_writing_a_payload_spanning_many_blocks(): void
    {
        $stream = $this
            ->sftpContext()
            ->filesystem(sftp_filesystem_options()->withBlockSize(1024))
            ->writeTo(path('sftp:///upload/blocks.txt'));

        for ($block = 0; $block < 10; $block++) {
            $stream->append(str_repeat((string) $block, 1024));
        }

        $stream->close();

        $content = $this->sftpContext()->contentOf(path('sftp:///upload/blocks.txt'));

        static::assertSame(10 * 1024, strlen($content));
        static::assertSame(str_repeat('0', 1024) . str_repeat('1', 1024), substr($content, 0, 2048));
        static::assertSame(str_repeat('9', 1024), substr($content, -1024));
    }

    public function test_writing_from_a_resource(): void
    {
        $resource = fopen('php://temp', 'r+b');
        static::assertNotFalse($resource);
        fwrite($resource, $content = str_repeat("id,name\n1,one\n", 500));
        rewind($resource);

        $stream = $this
            ->sftpContext()
            ->filesystem(sftp_filesystem_options()->withBlockSize(1024))
            ->writeTo(path('sftp:///upload/resource.csv'));
        $stream->fromResource($resource);
        $stream->close();
        fclose($resource);

        static::assertSame($content, $this->sftpContext()->contentOf(path('sftp:///upload/resource.csv')));
    }

    public function test_writing_over_a_longer_file_truncates_it(): void
    {
        $this->sftpContext()->givenFileExists(path('sftp:///upload/orders.csv'), str_repeat('x', 1000));

        $stream = $this->sftpContext()->filesystem()->writeTo(path('sftp:///upload/orders.csv'));
        $stream->append('short');
        $stream->close();

        static::assertSame('short', $this->sftpContext()->contentOf(path('sftp:///upload/orders.csv')));
    }

    public function test_writing_to_a_path_whose_directory_does_not_exist_yet(): void
    {
        $stream = $this->sftpContext()->filesystem()->writeTo(path('sftp:///upload/2024/01/orders.csv'));
        $stream->append("id\n1\n");
        $stream->close();

        static::assertSame("id\n1\n", $this->sftpContext()->contentOf(path('sftp:///upload/2024/01/orders.csv')));
    }
}
