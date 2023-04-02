<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

final class ToDateTime implements Expression
{
    public function __construct(
        private readonly Expression $ref,
        private readonly string $format,
        private readonly \DateTimeZone $timeZone = new \DateTimeZone('UTC')
    ) {
    }

    /**
     * @psalm-suppress MixedMethodCall
     */
    public function eval(Row $row) : mixed
    {
        /** @var mixed $value */
        $value = (new Row\Reference\ValueExtractor())->value($row, $this->ref);

        if (\is_object($value)) {
            return match (\get_class($value)) {
                /** @phpstan-ignore-next-line */
                \DateTimeImmutable::class, \DateTime::class => $value->setTimezone($this->timeZone),
                default => throw new \InvalidArgumentException('Entry ' . \get_class($value) . ' is not a DateTimeImmutable|DateTime')
            };
        }

        if (\is_int($value)) {
            return \DateTimeImmutable::createFromFormat('U', (string) $value, $this->timeZone);
        }

        if (\is_string($value)) {
            return \DateTimeImmutable::createFromFormat($this->format, $value, $this->timeZone);
        }

        throw new \InvalidArgumentException('Value ' . \gettype($value) . ' is not a DateTimeImmutable|DateTime|string|int');
    }
}
