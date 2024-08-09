<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Multiply extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|int|float $left,
        private readonly ScalarFunction|int|float $right
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $left = (new Parameter($this->left))->asNumber($row);
        $right = (new Parameter($this->right))->asNumber($row);

        if ($left === null || $right === null) {
            return null;
        }

        return $left * $right;
    }
}
