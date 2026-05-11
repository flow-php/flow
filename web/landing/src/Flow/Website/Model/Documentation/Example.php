<?php

declare(strict_types=1);

namespace Flow\Website\Model\Documentation;

final readonly class Example
{
    public function __construct(
        public string $topic,
        public string $name,
        public ?string $option = null,
    ) {}
}
