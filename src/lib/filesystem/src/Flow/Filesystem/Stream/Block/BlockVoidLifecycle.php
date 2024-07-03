<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream\Block;

use Flow\Filesystem\Stream\{Block, BlockLifecycle};

final class BlockVoidLifecycle implements BlockLifecycle
{
    public function create(int $size, Block $block) : void
    {
    }

    public function filled(Block $block) : void
    {
    }
}
