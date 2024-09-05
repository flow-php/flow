<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{NativePHPRandomValueGenerator, RandomValueGenerator, Row};

class RandomString implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction|int $length,
        private readonly RandomValueGenerator $generator = new NativePHPRandomValueGenerator(),
    ) {
    }

    public function eval(Row $row) : ?string
    {
        $length = (new Parameter($this->length))->asInt($row);

        if ($length === null) {
            return null;
        }

        return $this->generator->string($length);
    }
}
