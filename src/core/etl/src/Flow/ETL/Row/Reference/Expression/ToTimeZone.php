<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ToTimeZone implements Expression
{
    public function __construct(
        private readonly Expression $expression,
        private readonly Expression $timezone
    ) {
    }

    public function eval(Row $row) : mixed
    {
        /**
         * @var mixed $dateTime
         */
        $dateTime = $this->expression->eval($row);
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
