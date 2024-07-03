<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Unit;

use Flow\Azure\SDK\BlobService\BlockBlob\BlockList;
use Flow\Azure\SDK\BlobService\PutBlockBlob\PutBlockBlobOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlock\PutBlockBlobBlockOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\PutBlockBlobBlockListOptions;
use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Filesystem\Bridge\Azure\AzureBlobDestinationStream;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\{Block, BlockFactory};
use PHPUnit\Framework\TestCase;

final class AzureBlobDestinationStreamTest extends TestCase
{
    public function test_using_put_blob_with_content_when_data_is_larger_than_block_size() : void
    {
        $blockSize = 100;

        $blockFactory = $this->createMock(BlockFactory::class);
        $blockFactory->method('create')
            ->willReturnCallback(
                function () use ($blockSize) {
                    return new Block($id = \bin2hex(\random_bytes(16)), $blockSize, new Path(sys_get_temp_dir() . '/' . $id . '_block_01.txt'));
                }
            );

        $stream = AzureBlobDestinationStream::openBlank(
            $blobService = $this->createMock(BlobServiceInterface::class),
            new Path('azure-blob://file.txt'),
            $blockFactory,
            $blockSize
        );

        $blobService->expects(self::once())
            ->method('putBlockBlob')
            ->with(
                '/file.txt',
                self::isNull(),
                self::isNull(),
                self::isInstanceOf(PutBlockBlobOptions::class)
            );

        $blobService->expects(self::exactly(2))
            ->method('putBlockBlobBlock')
            ->with(
                '/file.txt',
                self::isType('string'),
                self::isType('resource'),
                self::isType('int'),
                self::isInstanceOf(PutBlockBlobBlockOptions::class)
            );

        $blobService->expects(self::once())
            ->method('putBlockBlobBlockList')
            ->with(
                '/file.txt',
                self::isInstanceOf(BlockList::class),
                self::isInstanceOf(PutBlockBlobBlockListOptions::class)
            );

        $stream->append(\str_repeat('a', 150));

        $stream->close();
    }

    public function test_using_put_blob_with_content_when_data_is_smaller_than_block_size() : void
    {
        $blockSize = 100;
        $blockFactory = $this->createMock(BlockFactory::class);
        $blockFactory->method('create')
            ->willReturnCallback(
                function () use ($blockSize) {
                    return new Block($id = \bin2hex(\random_bytes(16)), $blockSize, new Path(sys_get_temp_dir() . '/' . $id . '_block_01.txt'));
                }
            );
        $stream = AzureBlobDestinationStream::openBlank(
            $blobService = $this->createMock(BlobServiceInterface::class),
            new Path('azure-blob://file.txt'),
            $blockFactory,
            $blockSize
        );

        $blobService->expects(self::once())
            ->method('putBlockBlob')
            ->with(
                '/file.txt',
                self::isType('resource'),
                \strlen('Hello, World!'),
                self::isInstanceOf(PutBlockBlobOptions::class)
            );

        $stream->append('Hello, World!');

        $stream->close();
    }
}
