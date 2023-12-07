<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Fixtures;

final class IntObject
{
    private int $value = 0;

    public function __construct()
    {
    }

    public function add(int|Literal $a) : self
    {
        $this->value += ($a instanceof Literal) ? $a->value : $a;

        return $this;
    }

    public function set(int|Literal $a) : self
    {
        $this->value = ($a instanceof Literal) ? $a->value : $a;

        return $this;
    }

    public function value() : int
    {
        return $this->value;
    }
}
