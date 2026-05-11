<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Unit;

use Flow\Azure\SDK\BlobService\BlockBlob\BlockList;
use Flow\Azure\SDK\BlobService\PutBlockBlob\PutBlockBlobOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlock\PutBlockBlobBlockOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\PutBlockBlobBlockListOptions;
use Flow\Azure\SDK\BlobServiceInterface;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\Azure\AzureBlobDestinationStream;
use Flow\Filesystem\Stream\Block;
use Flow\Filesystem\Stream\BlockFactory;

use function Flow\ETL\DSL\generate_random_string;
use function Flow\Filesystem\DSL\path;

final class AzureBlobDestinationStreamTest extends FlowTestCase
{
    public function test_using_put_blob_with_content_when_data_is_larger_than_block_size(): void
    {
        $blockSize = 100;

        $blockFactory = $this->createMock(BlockFactory::class);
        $blockFactory
            ->method('create')
            ->willReturnCallback(
                static fn() => new Block(
                    $id = generate_random_string(),
                    $blockSize,
                    path(sys_get_temp_dir() . '/' . $id . '_block_01.txt'),
                ),
            );

        $stream = AzureBlobDestinationStream::openBlank(
            $blobService = $this->createMock(BlobServiceInterface::class),
            path('azure-blob://file.txt'),
            $blockFactory,
            $blockSize,
        );

        $blobService
            ->expects(self::once())
            ->method('putBlockBlob')
            ->with('/file.txt', static::isNull(), static::isNull(), static::isInstanceOf(PutBlockBlobOptions::class));

        $blobService
            ->expects(self::exactly(2))
            ->method('putBlockBlobBlock')
            ->with(
                '/file.txt',
                static::isType('string'),
                static::isType('resource'),
                static::isType('int'),
                static::isInstanceOf(PutBlockBlobBlockOptions::class),
            );

        $blobService
            ->expects(self::once())
            ->method('putBlockBlobBlockList')
            ->with(
                '/file.txt',
                static::isInstanceOf(BlockList::class),
                static::isInstanceOf(PutBlockBlobBlockListOptions::class),
            );

        $stream->append(\str_repeat('a', 150));

        $stream->close();
    }

    public function test_using_put_blob_with_content_when_data_is_smaller_than_block_size(): void
    {
        $blockSize = 100;
        $blockFactory = $this->createMock(BlockFactory::class);
        $blockFactory
            ->method('create')
            ->willReturnCallback(
                static fn() => new Block(
                    $id = generate_random_string(),
                    $blockSize,
                    path(sys_get_temp_dir() . '/' . $id . '_block_01.txt'),
                ),
            );
        $stream = AzureBlobDestinationStream::openBlank(
            $blobService = $this->createMock(BlobServiceInterface::class),
            path('azure-blob://file.txt'),
            $blockFactory,
            $blockSize,
        );

        $blobService
            ->expects(self::once())
            ->method('putBlockBlob')
            ->with(
                '/file.txt',
                static::isType('resource'),
                \strlen('Hello, World!'),
                static::isInstanceOf(PutBlockBlobOptions::class),
            );

        $stream->append('Hello, World!');

        $stream->close();
    }
}
