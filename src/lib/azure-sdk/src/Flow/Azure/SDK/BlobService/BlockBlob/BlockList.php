<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\BlockBlob;

final class BlockList
{
    /**
     * @var array<Block>
     */
    private array $blocks;

    public function __construct(Block ...$blocks)
    {
        $this->blocks = $blocks;
    }

    public function all() : array
    {
        return $this->blocks;
    }

    public function append(Block $block) : self
    {
        $this->blocks[] = $block;

        return $this;
    }

    public function last() : ?Block
    {
        if (!\count($this->blocks)) {
            return null;
        }

        $blocks = $this->blocks;

        return \array_pop($blocks);
    }
}
