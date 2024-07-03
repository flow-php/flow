<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\AzureBlobDestinationStream;

use Flow\Azure\SDK\BlobService\BlockBlob\{BlockList, BlockState};
use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\{Block, BlockLifecycle};

final class AzureBlobBlockLifecycle implements BlockLifecycle
{
    private bool $initialized = false;

    public function __construct(
        private readonly BlobServiceInterface $blobService,
        private readonly Path $path,
        private readonly BlockList $blockList
    ) {
        if (\count($this->blockList->all())) {
            $this->initialized = true;
        }
    }

    public function filled(Block $block) : void
    {
        if (!$this->initialized) {
            $this->blobService->putBlockBlob($this->path->path());
            $this->initialized = true;
        }

        $handle = \fopen($block->path()->path(), 'rb');

        if ($handle === false) {
            throw new RuntimeException('Cannot open block file for reading');
        }

        $this->blobService->putBlockBlobBlock(
            $this->path->path(),
            $block->id(),
            $handle,
            $block->size(),
        );

        /** @psalm-suppress RedundantCondition */
        if (\is_resource($handle)) {
            \fclose($handle);
        }

        \unlink($block->path()->path());

        $this->blockList->append(new \Flow\Azure\SDK\BlobService\BlockBlob\Block(
            $block->id(),
            BlockState::UNCOMMITTED,
        ));
    }
}
