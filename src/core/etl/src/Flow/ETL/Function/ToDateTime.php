<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{type_datetime};
use Flow\ETL\Function\ScalarFunction\TypedScalarFunction;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row;

final class ToDateTime extends ScalarFunctionChain implements TypedScalarFunction
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
            return \DateTimeImmutable::createFromFormat('U', (string) $value, $timeZone);
        }

        if (\is_string($value)) {
            return \DateTimeImmutable::createFromFormat($format, $value, $timeZone);
        }

        return null;
    }

    public function returns() : Type
    {
        return type_datetime();
    }
}
