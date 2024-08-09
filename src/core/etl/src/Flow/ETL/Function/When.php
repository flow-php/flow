<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class When extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $condition,
        private readonly mixed $then,
        private readonly mixed $else = null
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $condition = (new Parameter($this->condition))->asBoolean($row);

        if ($condition) {
            return (new Parameter($this->then))->eval($row);
        }

        if ($this->else) {
            return (new Parameter($this->else))->eval($row);
        }

        return null;
    }
}
