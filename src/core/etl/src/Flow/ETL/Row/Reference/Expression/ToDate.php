<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ToDate implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly string $format,
        private readonly \DateTimeZone $timeZone = new \DateTimeZone('UTC')
    ) {
    }

    /**
     * @psalm-suppress PossiblyFalseReference
     */
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
            /** @phpstan-ignore-next-line */
            return \DateTimeImmutable::createFromFormat('U', (string) $value, $this->timeZone)->setTime(0, 0, 0, 0);
        }

        if (\is_string($value)) {
            /** @phpstan-ignore-next-line */
            return \DateTimeImmutable::createFromFormat($this->format, $value, $this->timeZone)->setTime(0, 0, 0, 0);
        }

        return null;
    }
}
