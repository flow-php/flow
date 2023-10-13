<?php declare(strict_types=1);

namespace Flow\Dremel;

final class NullNode implements Node
{
    public function __construct(private readonly int $level)
    {
    }

    public function repetition() : int
    {
        return $this->level;
    }

    public function value() : array|null
    {
        return null;
    }
}
