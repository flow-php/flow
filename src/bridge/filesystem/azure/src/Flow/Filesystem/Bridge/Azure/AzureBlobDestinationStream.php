<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobService\BlockBlob\BlockList;
use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\{Block, BlockFactory, Blocks};
use Flow\Filesystem\{Bridge\Azure\AzureBlobDestinationStream\AzureBlobBlockLifecycle,
    DestinationStream,
    Exception\RuntimeException,
    Path};

final class AzureBlobDestinationStream implements DestinationStream
{
    private bool $closed = false;

    public function __construct(
        private readonly BlobServiceInterface $blobService,
        private readonly Path $path,
        private readonly Blocks $blocks,
        private readonly BlockList $blockList
    ) {
    }

    public static function openBlank(
        BlobServiceInterface $blobService,
        Path $path,
        BlockFactory $blockFactory = new Block\NativeLocalFileBlocksFactory(),
        int $blockSize = 1024 * 1024 * 4,
    ) : self {
        return new self(
            $blobService,
            $path,
            new Blocks(
                $blockSize,
                $blockFactory,
                new AzureBlobBlockLifecycle($blobService, $path, $blockList = new BlockList())
            ),
            $blockList
        );
    }

    public function append(string $data) : self
    {
        $this->blocks->append($data);

        return $this;
    }

    public function close() : void
    {
        if ($this->blocks->size() === 0) {
            $this->blobService->putBlockBlob($this->path->path());
            $this->closed = true;

            return;
        }

        if ($this->blocks->count() > 1) {
            $this->blocks->done();
            $this->blobService->putBlockBlobBlockList(
                $this->path->path(),
                $this->blockList
            );
        } else {
            $handle = \fopen($this->blocks->block()->path()->path(), 'rb');

            if ($handle === false) {
                throw new RuntimeException('Cannot open block file for reading');
            }

            $this->blobService->putBlockBlob($this->path->path(), $handle, $this->blocks->block()->size());

            /** @psalm-suppress RedundantCondition */
            if (\is_resource($handle)) {
                \fclose($handle);
            }

            \unlink($this->blocks->block()->path()->path());
        }

        $this->closed = true;
    }

    public function fromResource($resource) : self
    {
        if (!\is_resource($resource)) {
            throw new InvalidArgumentException('DestinationStream::fromResource expects resource type, given: ' . \gettype($resource));
        }

        $meta = \stream_get_meta_data($resource);

        if ($meta['seekable']) {
            \rewind($resource);
        }

        $this->blocks->fromResource($resource);

        return $this;
    }

    public function isOpen() : bool
    {
        return !$this->closed;
    }

    public function path() : Path
    {
        return $this->path;
    }
}
