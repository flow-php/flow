<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Step;

use Flow\ArrayDot\Step;

final readonly class Wildcard implements Step
{
    public function __construct(
        public bool $nullsafe = false,
    ) {}

    public function toString(): string
    {
        return $this->nullsafe ? '?*' : '*';
    }
}
