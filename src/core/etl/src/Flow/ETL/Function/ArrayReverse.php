<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ArrayReverse extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $left, private readonly bool $preserveKeys)
    {
    }

    public function eval(Row $row) : mixed
    {
        $left = $this->left->eval($row);

        if (!\is_array($left)) {
            return null;
        }

        return \array_reverse($left, $this->preserveKeys);
    }
}
