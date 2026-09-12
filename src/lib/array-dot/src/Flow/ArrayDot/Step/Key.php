<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Step;

use Flow\ArrayDot\Path;
use Flow\ArrayDot\Step;

use function addcslashes;

final readonly class Key implements Step
{
    public function __construct(
        public int|string $name,
        public bool $nullsafe = false,
    ) {}

    public function toString(): string
    {
        return ($this->nullsafe ? '?' : '') . addcslashes((string) $this->name, Path::ESCAPABLE);
    }
}
