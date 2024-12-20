<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

final class Page
{
    public function __construct(
        public readonly string $path,
        public readonly string $content,
    ) {
    }
}
