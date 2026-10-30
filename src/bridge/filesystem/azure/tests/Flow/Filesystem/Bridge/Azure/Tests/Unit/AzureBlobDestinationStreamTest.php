<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Unit;

use Flow\Azure\SDK\BlobService\BlockBlob\BlockList;
use Flow\Azure\SDK\BlobService\PutBlockBlob\PutBlockBlobOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlock\PutBlockBlobBlockOptions;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\PutBlockBlobBlockListOptions;
use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Azure\SDK\Exception\AzureException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Bridge\Azure\AzureBlobDestinationStream;
use Flow\Filesystem\Exception\FileExistsException;
use Flow\Filesystem\Stream\Block;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;
use Http\Discovery\Psr17FactoryDiscovery;

use function bin2hex;
use function Flow\ETL\DSL\generate_random_string;
use function Flow\Filesystem\DSL\path;
use function glob;
use function random_bytes;
use function rmdir;
use function str_repeat;
use function strlen;

final class AzureBlobDestinationStreamTest extends FlowTestCase
{
    public function test_exclusive_stream_refused_with_one_block_removes_the_block_file(): void
    {
        $blocks = sys_get_temp_dir() . '/flow-azure-blocks-' . bin2hex(random_bytes(4));
        $stream = AzureBlobDestinationStream::openExclusive(
            $blobService = $this->createMock(BlobServiceInterface::class),
            path('azure-blob://file.txt'),
            new NativeLocalFileBlocksFactory($blocks),
            100,
        );
        $blobService
            ->expects(self::once())
            ->method('putBlockBlob')
            ->willThrowException(
                new AzureException(
                    'putBlockBlob',
                    Psr17FactoryDiscovery::findRequestFactory()->createRequest('PUT', 'http://azurite/file.txt'),
                    Psr17FactoryDiscovery::findResponseFactory()->createResponse(409),
                ),
            );
        $stream->append('secret');

        try {
            $stream->close();
            static::fail('Expected the existing blob to be refused');
        } catch (FileExistsException) {
        }

        $stream->close();

        static::assertSame([], glob($blocks . '/*'));

        rmdir($blocks);
    }

    public function test_exclusive_stream_with_many_blocks_commits_the_block_list_if_none_match(): void
    {
        $stream = AzureBlobDestinationStream::openExclusive(
            $blobService = $this->createMock(BlobServiceInterface::class),
            path('azure-blob://file.txt'),
            new NativeLocalFileBlocksFactory(),
            100,
        );
        $blobService
            ->expects(self::once())
            ->method('putBlockBlobBlockList')
            ->with(
                '/file.txt',
                static::isInstanceOf(BlockList::class),
                static::callback(
                    static fn(PutBlockBlobBlockListOptions $options): bool => (
                        $options->toHeaders()['If-None-Match'] === '*'
                    ),
                ),
            )
            ->willThrowException(
                new AzureException(
                    'putBlockBlobBlockList',
                    Psr17FactoryDiscovery::findRequestFactory()->createRequest('PUT', 'http://azurite/file.txt'),
                    Psr17FactoryDiscovery::findResponseFactory()->createResponse(409),
                ),
            );
        $stream->append(str_repeat('a', 250));

        $this->expectException(FileExistsException::class);

        $stream->close();
    }

    public function test_second_close_is_a_no_op(): void
    {
        $stream = AzureBlobDestinationStream::openBlank(
            $blobService = $this->createMock(BlobServiceInterface::class),
            path('azure-blob://file.txt'),
            new NativeLocalFileBlocksFactory(),
            100,
        );
        $blobService->expects(self::once())->method('putBlockBlob');
        $stream->append('a');

        $stream->close();
        $stream->close();
    }

    public function test_using_put_blob_with_content_when_data_is_larger_than_block_size(): void
    {
        $blockSize = 100;

        $blockFactory = $this->createStub(BlockFactory::class);
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

        $blobService->expects(self::never())->method('putBlockBlob');

        $blobService
            ->expects(self::exactly(2))
            ->method('putBlockBlobBlock')
            ->with(
                '/file.txt',
                static::isString(),
                static::isResource(),
                static::isInt(),
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

        $stream->append(str_repeat('a', 150));

        $stream->close();
    }

    public function test_using_put_blob_with_content_when_data_is_smaller_than_block_size(): void
    {
        $blockSize = 100;
        $blockFactory = $this->createStub(BlockFactory::class);
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
                static::isResource(),
                strlen('Hello, World!'),
                static::isInstanceOf(PutBlockBlobOptions::class),
            );

        $stream->append('Hello, World!');

        $stream->close();
    }
}
