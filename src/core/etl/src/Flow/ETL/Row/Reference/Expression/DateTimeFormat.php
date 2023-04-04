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
            throw new \InvalidArgumentException('Entry ' . \gettype($value) . ' is not a DateTimeEntry');
        }

        return $value->format($this->format);
    }
}
