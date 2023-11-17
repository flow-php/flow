<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ToTimeZone implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction $function,
        private readonly ScalarFunction $timezone
    ) {
    }

    public function eval(Row $row) : mixed
    {
        /**
         * @var mixed $dateTime
         */
        $dateTime = $this->function->eval($row);
        /**
         * @var mixed $tz
         */
        $tz = $this->timezone->eval($row);

        if (!$dateTime instanceof \DateTime && !$dateTime instanceof \DateTimeImmutable) {
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

        return $dateTime->setTimezone($tz);
    }
}
