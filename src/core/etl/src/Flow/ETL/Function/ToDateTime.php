<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ToDateTime implements ScalarFunction
{
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly string $format,
        private readonly \DateTimeZone $timeZone = new \DateTimeZone('UTC')
    ) {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = $this->ref->eval($row);

        if (\is_object($value)) {
            if (\is_a($value, \DateTimeImmutable::class) || \is_a($value, \DateTime::class)) {
                return $value->setTimezone($this->timeZone)->setTime(0, 0, 0, 0);
            }

            return null;
        }

        if (\is_int($value)) {
            return \DateTimeImmutable::createFromFormat('U', (string) $value, $this->timeZone);
        }

        if (\is_string($value)) {
            return \DateTimeImmutable::createFromFormat($this->format, $value, $this->timeZone);
        }

        return null;
    }
}
