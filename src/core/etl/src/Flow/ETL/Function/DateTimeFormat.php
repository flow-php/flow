<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class DateTimeFormat implements ScalarFunction
{
    use EntryScalarFunction;

    public function __construct(
        private readonly ScalarFunction $ref,
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
