<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ToTimeZone extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction|\DateTimeInterface $value,
        private readonly ScalarFunction|\DateTimeZone|string $timezone
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $dateTime = (new Parameter($this->value))->asInstanceOf($row, \DateTimeInterface::class);
        $tz = Parameter::oneOf(
            (new Parameter($this->timezone))->asString($row),
            (new Parameter($this->timezone))->asInstanceOf($row, \DateTimeZone::class)
        );

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
