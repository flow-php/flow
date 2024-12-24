<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Integration;

use function Flow\Filesystem\Bridge\Azure\DSL\{azure_filesystem, azure_filesystem_options};
use Flow\Filesystem\Bridge\Azure\Options;
use Flow\Filesystem\Path;

final class AzureBlobFilesystemTest extends AzureBlobServiceTestCase
{
    public function test_appending_to_existing_blob() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $stream = $fs->writeTo(new Path('azure-blob://file.txt'));
        $stream->append("This is first line\n");
        $stream->close();

        $stream = $fs->appendTo(new Path('azure-blob://file.txt'));
        $stream->append("This is second line\n");
        $stream->close();

        self::assertTrue($fs->status(new Path('azure-blob://file.txt'))->isFile());
        self::assertFalse($fs->status(new Path('azure-blob://file.txt'))->isDirectory());
        self::assertSame(
            <<<'TXT'
This is first line
This is second line

TXT
            ,
            $fs->readFrom(new Path('azure-blob://file.txt'))->content()
        );

        $fs->rm(new Path('azure-blob://file.txt'));
    }

    public function test_appending_to_existing_block_blob_new_blocks() : void
    {
        $fs = azure_filesystem(
            $this->blobService('flow-php'),
            (new Options())->withBlockSize(1024)
        );

        $stream = $fs->writeTo(new Path('azure-blob://file.txt'));
        $output = '';

        for ($i = 0; $i < 10; $i++) {
            $output .= \str_repeat('a', 1024) . "\n";
            $stream->append(\str_repeat('a', 1024) . "\n");
        }
        $stream->close();

        $stream = $fs->appendTo(new Path('azure-blob://file.txt'));

        for ($i = 0; $i < 10; $i++) {
            $output .= \str_repeat('n', 1024) . "\n";
            $stream->append(\str_repeat('n', 1024) . "\n");
        }
        $stream->close();

        self::assertTrue($fs->status(new Path('azure-blob://file.txt'))->isFile());
        self::assertFalse($fs->status(new Path('azure-blob://file.txt'))->isDirectory());
        self::assertSame(
            $output,
            $fs->readFrom(new Path('azure-blob://file.txt'))->content()
        );

        $fs->rm(new Path('azure-blob://file.txt'));
    }

    public function test_appending_to_existing_non_block_blob_new_blocks() : void
    {
        $fs = azure_filesystem(
            $this->blobService('flow-php'),
            (new Options())->withBlockSize(1024)
        );

        $stream = $fs->writeTo(new Path('azure-blob://file.txt'));
        $stream->append("This is first line\n");
        $stream->close();

        $stream = $fs->appendTo(new Path('azure-blob://file.txt'));
        $output = "This is first line\n";

        for ($i = 0; $i < 10; $i++) {
            $output .= \str_repeat('a', 1024) . "\n";
            $stream->append(\str_repeat('a', 1024) . "\n");
        }
        $stream->close();

        self::assertTrue($fs->status(new Path('azure-blob://file.txt'))->isFile());
        self::assertFalse($fs->status(new Path('azure-blob://file.txt'))->isDirectory());
        self::assertSame(
            $output,
            $fs->readFrom(new Path('azure-blob://file.txt'))->content()
        );

        $fs->rm(new Path('azure-blob://file.txt'));
    }

    public function test_file_status_on_existing_file() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $fs->writeTo(new Path('azure-blob://file.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();

        self::assertTrue($fs->status(new Path('azure-blob://file.txt'))->isFile());
    }

    public function test_file_status_on_existing_folder() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $fs->writeTo(new Path('azure-blob://nested/orders/orders.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();

        self::assertTrue($fs->status(new Path('azure-blob://nested/orders'))->isDirectory());
        self::assertTrue($fs->status(new Path('azure-blob://nested/orders/'))->isDirectory());
    }

    public function test_file_status_on_non_existing_file() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        self::assertNull($fs->status(new Path('azure-blob://non-existing-file.txt')));
    }

    public function test_file_status_on_non_existing_folder() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        self::assertNull($fs->status(new Path('azure-blob://non-existing-folder/')));
    }

    public function test_file_status_on_non_existing_pattern() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        self::assertNull($fs->status(new Path('azure-blob://non-existing-folder/*')));
    }

    public function test_file_status_on_partial_path() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $fs->writeTo(new Path('azure-blob://some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();

        self::assertNull($fs->status(new Path('azure-blob://some_path')));
    }

    public function test_file_status_on_pattern() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $fs->writeTo(new Path('azure-blob://some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();

        self::assertTrue($fs->status(new Path('azure-blob://some_path_to/*.txt'))->isFile());
        self::assertSame('azure-blob://some_path_to/file.txt', $fs->status(new Path('azure-blob://some_path_to/*.txt'))->path->uri());
    }

    public function test_file_status_on_root_folder() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        self::assertTrue($fs->status(new Path('azure-blob://'))->isDirectory());
    }

    public function test_move_blob() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $fs->writeTo(new Path('azure-blob://file.txt'))->append('Hello, World!')->close();

        $fs->mv(new Path('azure-blob://file.txt'), new Path('azure-blob://file_mv.txt'));

        self::assertNull($fs->status(new Path('azure-blob://file.txt')));
        self::assertSame('Hello, World!', $fs->readFrom(new Path('azure-blob://file_mv.txt'))->content());
    }

    public function test_not_removing_a_content_when_its_not_a_full_folder_path_pattern() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $fs->writeTo(new Path('azure-blob://nested/orders/orders.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(new Path('azure-blob://nested/orders/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(new Path('azure-blob://nested/orders/orders_01.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();

        self::assertTrue($fs->status(new Path('azure-blob://nested/orders/orders.csv'))->isFile());
        self::assertTrue($fs->status(new Path('azure-blob://nested/orders/orders_01.csv'))->isFile());

        self::assertFalse($fs->rm(new Path('azure-blob://nested/orders/ord')));
    }

    public function test_removing_folder() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $fs->writeTo(new Path('azure-blob://orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(new Path('azure-blob://nested/orders/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(new Path('azure-blob://nested/orders/orders_01.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();

        self::assertTrue($fs->status(new Path('azure-blob://nested/orders/orders.csv'))->isFile());
        self::assertTrue($fs->status(new Path('azure-blob://nested/orders/orders_01.csv'))->isFile());

        $fs->rm(new Path('azure-blob://nested/orders'));

        self::assertTrue($fs->status(new Path('azure-blob://orders.csv'))->isFile());
        self::assertNull($fs->status(new Path('azure-blob://nested/orders/orders.csv')));
        self::assertNull($fs->status(new Path('azure-blob://nested/orders/orders_01.csv')));
    }

    public function test_removing_folder_pattern() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $fs->writeTo(new Path('azure-blob://nested/orders/orders.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(new Path('azure-blob://nested/orders/orders.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();
        $fs->writeTo(new Path('azure-blob://nested/orders/orders_01.csv'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'))->close();

        self::assertTrue($fs->status(new Path('azure-blob://nested/orders/orders.csv'))->isFile());
        self::assertTrue($fs->status(new Path('azure-blob://nested/orders/orders_01.csv'))->isFile());

        $fs->rm(new Path('azure-blob://nested/orders/*.csv'));

        self::assertTrue($fs->status(new Path('azure-blob://nested/orders/orders.txt'))->isFile());
        self::assertNull($fs->status(new Path('azure-blob://nested/orders/orders.csv')));
        self::assertNull($fs->status(new Path('azure-blob://nested/orders/orders_01.csv')));
    }

    public function test_rm_tmp_dir() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        self::assertFalse($fs->rm($fs->getSystemTmpDir()));
    }

    public function test_tmp_dir() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        self::assertSame('azure-blob://_$azure_flow_tmp$/', $fs->getSystemTmpDir()->uri());
    }

    public function test_tmp_dir_status() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        self::assertTrue($fs->status($fs->getSystemTmpDir())->isDirectory());
    }

    public function test_write_to_custom_tmp_dir() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'), azure_filesystem_options()->withTmpDir(new Path('azure-blob://custom-tmp-dir/')));

        $stream = $fs->writeTo($fs->getSystemTmpDir()->suffix('file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        self::assertTrue($fs->status(new Path('azure-blob://custom-tmp-dir/file.txt'))->isFile());
        self::assertSame('Hello, World!', $fs->readFrom(new Path('azure-blob://custom-tmp-dir/file.txt'))->content());

        $fs->rm($fs->getSystemTmpDir()->suffix('file.txt'));
    }

    public function test_write_to_tmp_dir() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $stream = $fs->writeTo($fs->getSystemTmpDir()->suffix('file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        self::assertTrue($fs->status($fs->getSystemTmpDir()->suffix('file.txt'))->isFile());
        self::assertSame('Hello, World!', $fs->readFrom($fs->getSystemTmpDir()->suffix('file.txt'))->content());

        $fs->rm($fs->getSystemTmpDir()->suffix('file.txt'));
    }

    public function test_write_to_tmp_dir_as_to_a_file() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $this->expectExceptionMessage('Cannot write to system tmp directory');

        $fs->writeTo($fs->getSystemTmpDir());
    }

    public function test_writing_to_azure_blob_storage() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $stream = $fs->writeTo(new Path('azure-blob://file.txt'));
        $stream->append('Hello, World!');
        $stream->close();

        self::assertTrue($fs->status(new Path('azure-blob://file.txt'))->isFile());
        self::assertFalse($fs->status(new Path('azure-blob://file.txt'))->isDirectory());
        self::assertSame('Hello, World!', $fs->readFrom(new Path('azure-blob://file.txt'))->content());

        $fs->rm(new Path('azure-blob://file.txt'));
    }

    public function test_writing_to_to_azure_from_resources() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $stream = $fs->writeTo(new Path('azure-blob://orders.csv'));
        $stream->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));
        $stream->close();

        self::assertTrue($fs->status(new Path('azure-blob://orders.csv'))->isFile());
        self::assertFalse($fs->status(new Path('azure-blob://orders.csv'))->isDirectory());
        self::assertSame(\file_get_contents(__DIR__ . '/Fixtures/orders.csv'), $fs->readFrom(new Path('azure-blob://orders.csv'))->content());

        $fs->rm(new Path('azure-blob://orders.csv'));
    }

    public function test_writing_to_to_azure_using_blocks() : void
    {
        $fs = azure_filesystem(
            $this->blobService('flow-php'),
            (new Options())->withBlockSize(1024)
        );

        $stream = $fs->writeTo(new Path('azure-blob://block_blob.csv'));

        for ($i = 0; $i < 10; $i++) {
            $stream->append(\str_repeat('a', 1024) . "\n");
        }

        $stream->close();

        self::assertTrue($fs->status(new Path('azure-blob://block_blob.csv'))->isFile());
        self::assertFalse($fs->status(new Path('azure-blob://block_blob.csv'))->isDirectory());

        $fs->rm(new Path('azure-blob://block_blob.csv'));
    }
}
