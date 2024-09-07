<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

final class Example
{
    public function __construct(
        public readonly string $topic,
        public readonly string $name,
    ) {
    }
}
