<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Integration;

use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use Flow\Filesystem\Bridge\Azure\{Options};
use Flow\Filesystem\Path;

final class AzureBlobDestinationStreamTest extends AzureBlobServiceTestCase
{
    public function test_closing_empty_stream() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'));
        $stream = $fs->writeTo(new Path('azure-blob://file.txt'));
        self::assertTrue($stream->isOpen());
        $stream->close();
        self::assertFalse($stream->isOpen());
    }

    public function test_writing_content_bigger_than_block_size_to_azure() : void
    {
        $fs = azure_filesystem($this->blobService('flow-php'), (new Options())->withBlockSize(100));

        $stream = $fs->writeTo(new Path('azure-blob://file.txt'));
        $stream->append($content = \str_repeat('a', 200));
        $stream->close();

        self::assertTrue($fs->status(new Path('azure-blob://file.txt'))->isFile());
        self::assertFalse($fs->status(new Path('azure-blob://file.txt'))->isDirectory());
        self::assertSame($content, $fs->readFrom(new Path('azure-blob://file.txt'))->content());

        $fs->rm(new Path('azure-blob://file.txt'));
    }

    public function test_writing_content_from_resource() : void
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

    public function test_writing_content_smaller_than_block_size_to_azure() : void
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
}
