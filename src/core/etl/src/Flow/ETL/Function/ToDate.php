<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

final class ToDate extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly ScalarFunction|string $format,
        private readonly ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC'),
    ) {
    }

    public function eval(Row $row) : mixed
    {
        $value = (new Parameter($this->value))->eval($row);
        $format = (new Parameter($this->format))->asString($row);
        $timeZone = (new Parameter($this->timeZone))->asInstanceOf($row, \DateTimeZone::class);

        if ($value === null || $format === null || $timeZone === null) {
            return null;
        }

        if (\is_object($value)) {
            if (\is_a($value, \DateTimeImmutable::class) || \is_a($value, \DateTime::class)) {
                return $value->setTimezone($timeZone)->setTime(0, 0, 0, 0);
            }

            return null;
        }

        if (\is_int($value)) {
            /** @phpstan-ignore-next-line */
            return \DateTimeImmutable::createFromFormat('U', (string) $value, $timeZone)->setTime(0, 0, 0, 0);
        }

        if (\is_string($value)) {
            /** @phpstan-ignore-next-line */
            return \DateTimeImmutable::createFromFormat($format, $value, $timeZone)->setTime(0, 0, 0, 0);
        }

        return null;
    }
}
