<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{NativePHPRandomValueGenerator, RandomValueGenerator, Row};

class RandomString implements ScalarFunction
{
    private RandomValueGenerator|NativePHPRandomValueGenerator $generator;

    private int|ScalarFunction $length;

    public function __construct(ScalarFunction|int $length, RandomValueGenerator $generator = new NativePHPRandomValueGenerator())
    {
        $this->length = $length;
        $this->generator = $generator;
    }

    public function eval(Row $row) : string
    {
        return $this->generator->string(is_int($this->length) ? $this->length : $this->length->eval($row));
    }
}
