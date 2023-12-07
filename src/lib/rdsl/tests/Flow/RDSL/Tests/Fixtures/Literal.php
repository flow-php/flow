<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Fixtures;

final class Literal
{
    public function __construct(public readonly mixed $value)
    {
    }
}
