<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Double;

final class InvokableObject
{
    public function __invoke(): int
    {
        return 1;
    }
}
