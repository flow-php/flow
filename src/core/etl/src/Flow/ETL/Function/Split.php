<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Split extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly ScalarFunction|string $separator,
        private readonly ScalarFunction|int $limit = PHP_INT_MAX,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $separator = $this->separator instanceof ScalarFunction ? $this->separator->eval($row) : $this->separator;
        $limit = $this->limit instanceof ScalarFunction ? $this->limit->eval($row) : $this->limit;

        $val = $this->ref->eval($row);

        if (!\is_string($val) || !\is_string($separator) || !\is_int($limit) || $limit < 1 || $separator === '') {
            return $val;
        }

        return \explode($separator, $val, $limit);
    }
}
