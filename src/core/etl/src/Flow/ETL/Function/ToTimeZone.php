<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_object, type_string};
use Flow\ETL\Row;

final class ToTimeZone extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DateTimeInterface $value,
        private readonly ScalarFunction|\DateTimeZone|string $timezone,
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $dateTime = (new Parameter($this->value))->asInstanceOf($row, \DateTimeInterface::class);
        $tz = (new Parameter($this->timezone))->as($row, type_string(), type_object(\DateTimeZone::class));

        if ($dateTime === null || $tz === null) {
            return null;
        }

        $tz = match (\gettype($tz)) {
            'string' => new \DateTimeZone($tz),
            'object' => $tz instanceof \DateTimeZone ? $tz : null,
            default => null,
        };

        if ($tz === null) {
            return null;
        }

        /** @var \DateTime|\DateTimeImmutable $dateTime */
        return $dateTime->setTimezone($tz);
    }
}
