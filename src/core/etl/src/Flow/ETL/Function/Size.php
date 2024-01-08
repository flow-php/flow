<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Size extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (\is_string($value)) {
            return \mb_strlen($value);
        }

        if (\is_countable($value)) {
            return \count($value);
        }

        return null;
    }
}
