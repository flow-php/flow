<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ToLower extends ScalarFunctionChain
{
    public function __construct(private ScalarFunction $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        return match (\gettype($value)) {
            'string' => \mb_strtolower($value),
            default => $value,
        };
    }
}
