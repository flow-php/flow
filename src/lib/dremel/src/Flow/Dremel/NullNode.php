<?php declare(strict_types=1);

namespace Flow\Dremel;

final class NullNode implements Node
{
    public function __construct()
    {
    }

    public function value() : array|null
    {
        return null;
    }
}
