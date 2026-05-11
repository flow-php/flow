<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class ToDateTime extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
        private readonly ScalarFunction|string $format,
        private readonly ScalarFunction|\DateTimeZone $timeZone = new \DateTimeZone('UTC'),
    ) {}

    public function eval(Row $row, FlowContext $context): \DateTimeImmutable|false|null
    {
        $value = (new Parameter($this->value))->eval($row, $context);
        $format = (new Parameter($this->format))->asString($row, $context);
        $timeZone = (new Parameter($this->timeZone))->asInstanceOf($row, $context, \DateTimeZone::class);

        if ($value === null || $format === null || $timeZone === null) {
            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('ToDateTime function requires non-null values'));
        }

        if (\is_object($value)) {
            if (\is_a($value, \DateTimeImmutable::class) || \is_a($value, \DateTime::class)) {
                $value = $value->setTimezone($timeZone)->setTime(0, 0, 0, 0);

                if ($value instanceof \DateTimeImmutable) {
                    return $value;
                }
            }

            return $context
                ->functions()
                ->invalidResult(new InvalidArgumentException('ToDateTime function requires DateTimeInterface object'));
        }

        if (\is_int($value)) {
            return \DateTimeImmutable::createFromFormat('U', (string) $value, $timeZone);
        }

        if (\is_string($value)) {
            return \DateTimeImmutable::createFromFormat($format, $value, $timeZone);
        }

        return $context
            ->functions()
            ->invalidResult(new InvalidArgumentException('ToDateTime function requires int or string value'));
    }
}
