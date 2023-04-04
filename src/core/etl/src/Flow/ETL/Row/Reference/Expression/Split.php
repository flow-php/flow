<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Split implements Expression
{
    /**
     * @param non-empty-string $separator
     */
    public function __construct(
        private readonly Expression $ref,
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
