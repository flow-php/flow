<?php

declare(strict_types=1);

namespace Flow\RDSL\Tests\Fixtures;

final readonly class Literal
{
    public function __construct(public mixed $value)
    {
    }
}
