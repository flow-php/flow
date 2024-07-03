<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Integration;

use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use Flow\Filesystem\Path;

final class AzureBlobFilesystemTest extends AzureBlobServiceTestCase
{
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

        $fs->writeTo(new Path('azure-blob://some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));

        self::assertNull($fs->status(new Path('azure-blob://some_path')));
    }

    public function test_file_status_on_pattern() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));

        $fs->writeTo(new Path('azure-blob://some_path_to/file.txt'))->fromResource(\fopen(__DIR__ . '/Fixtures/orders.csv', 'rb'));

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
}
