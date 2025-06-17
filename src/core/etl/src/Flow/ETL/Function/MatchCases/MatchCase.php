<?php

declare(strict_types=1);

namespace Flow\ETL\Function\MatchCases;

use Flow\ETL\Function\{Parameter, ScalarFunction};
use Flow\ETL\Row;

final readonly class MatchCase implements ScalarFunction
{
    public function __construct(
        private mixed $condition,
        private mixed $then,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        return (new Parameter($this->then))->eval($row);
    }

    public function valid(Row $row) : bool
    {
        return (new Parameter($this->condition))->asBoolean($row);
    }
}
