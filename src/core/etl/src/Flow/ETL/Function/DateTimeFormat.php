<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class DateTimeFormat extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DateTimeInterface $dateTime,
        private readonly ScalarFunction|string $format,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->dateTime))->asInstanceOf($row, \DateTimeInterface::class);
        $format = (new Parameter($this->format))->asString($row);

        if ($value === null || $format === null) {
            return null;
        }

        return $value->format($format);
    }
}
