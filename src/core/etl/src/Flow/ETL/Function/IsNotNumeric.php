<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class IsNotNumeric extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value
    ) {
    }

    public function eval(Row $row) : bool
    {
        return (new Parameter($this->value))->asNumber($row) === null;
    }
}
