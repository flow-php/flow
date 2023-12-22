<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class Split implements ScalarFunction
{
    use EntryScalarFunction;

    /**
     * @param non-empty-string $separator
     */
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly string $separator,
        private readonly int $limit = PHP_INT_MAX,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $val = $this->ref->eval($row);

        if (!\is_string($val)) {
            return $val;
        }

        return \explode($this->separator, $val, $this->limit);
    }
}
