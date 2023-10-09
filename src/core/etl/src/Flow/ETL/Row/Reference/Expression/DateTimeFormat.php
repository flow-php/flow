<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class DateTimeFormat implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly string $format
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = $this->ref->eval($row);

        if (!$value instanceof \DateTimeInterface) {
            return null;
        }

        return $value->format($this->format);
    }
}
