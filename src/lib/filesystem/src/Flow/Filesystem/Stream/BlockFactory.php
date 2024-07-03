<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

interface BlockFactory
{
    public function create(int $size) : Block;
}
