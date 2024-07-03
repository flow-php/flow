<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

interface BlockLifecycle
{
    public function filled(Block $block) : void;
}
