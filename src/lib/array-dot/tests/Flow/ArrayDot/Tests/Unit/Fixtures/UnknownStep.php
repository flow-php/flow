<?php

declare(strict_types=1);

namespace Flow\ArrayDot\Tests\Unit\Fixtures;

use Flow\ArrayDot\Step;

final readonly class UnknownStep implements Step
{
    public function toString(): string
    {
        return 'unknown';
    }
}
