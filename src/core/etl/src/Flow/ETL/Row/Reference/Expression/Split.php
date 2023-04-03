<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class Split implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly string $separator,
        private readonly int $limit = PHP_INT_MAX,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $val = (new Row\Reference\ValueExtractor())->value($row, $this->ref);

        if (!\is_string($val)) {
            return $val;
        }

        return \explode($this->separator, $val, $this->limit);
    }
}
