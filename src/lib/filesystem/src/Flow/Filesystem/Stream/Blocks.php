<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\Block\{BlockVoidLifecycle, NativeLocalFileBlocksFactory};

/**
 * Blocks is a collection of blocks that are filled with data.
 * Each file can be created from a single block or multiple blocks.
 * Blocks are mostly used by remote filesystems as a performance optimization technique.
 * Filesystem is appending into the blocks, whenever Block is full BlockLifecycle::filled(Block $block) is triggered
 * which allows to upload the block to the remote filesystem.
 *
 * Blocks are created by BlockFactory and filled with data by the Blocks collection.
 * in most cases BlockFactory implementation is just creating blocks in a local filesystem tmp directory.
 */
final class Blocks
{
    /**
     * @var Block[]
     */
    private array $blocks = [];

    private Block $currentBlock;

    private int $size = 0;

    /**
     * @param BlockFactory $blockFactory
     * @param int $blockSize block size in bytes
     */
    public function __construct(
        private readonly int $blockSize,
        private readonly BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        private readonly BlockLifecycle $blockLifecycle = new BlockVoidLifecycle(),
    ) {
        $this->currentBlock = $this->blockFactory->create($this->blockSize);
    }

    public function all() : array
    {
        return \array_merge($this->blocks, [$this->currentBlock]);
    }

    public function append(string $data) : void
    {
        /**
         * @phpstan-ignore-next-line
         */
        foreach (\str_split($data, $this->blockSize) as $chunk) {
            if ($this->block()->spaceLeft() < \strlen($chunk)) {
                // cut the chunk to fit into the block, store it in the block and move remaining part to next block
                $spaceLeft = $this->block()->spaceLeft();
                $this->block()->append(\substr($chunk, 0, $spaceLeft));
                $this->block()->append(\substr($chunk, $spaceLeft));
            } else {
                $this->block()->append($chunk);
            }
        }

        $this->size += \strlen($data);
    }

    /**
     * @return Block - current block that might not be filled yet
     */
    public function block() : Block
    {
        if ($this->currentBlock->spaceLeft() === 0) {
            $this->blocks[] = $this->currentBlock;
            $this->blockLifecycle->filled($this->currentBlock);
            $this->currentBlock = $this->blockFactory->create($this->blockSize);
        }

        return $this->currentBlock;
    }

    public function count() : int
    {
        return \count($this->blocks) + 1;
    }

    public function done() : void
    {
        if ($this->currentBlock->size() === 0) {
            return;
        }

        $this->blocks[] = $this->currentBlock;
        $this->blockLifecycle->filled($this->currentBlock);
    }

    /**
     * @param resource $resource
     */
    public function fromResource($resource) : void
    {
        if (!\is_resource($resource)) {
            throw new InvalidArgumentException('DestinationStream::fromResource expects resource type, given: ' . \gettype($resource));
        }

        // use Block::fromStream and simply move offset after each block
        $offset = \ftell($resource);

        if ($offset === false) {
            throw new InvalidArgumentException('Cannot determine current position in the stream');
        }

        while (!\feof($resource)) {
            $bytesCopied = $this->block()->fromResource($resource, $offset);
            $offset += $bytesCopied;
            $this->size += $bytesCopied;
        }
    }

    public function size() : int
    {
        return $this->size;
    }
}
