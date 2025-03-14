<?php

declare(strict_types=1);

namespace Flow\CLI\Formatter;

final readonly class ValueFormatter
{
    public function __construct(private string $nullValue = '-')
    {
    }

    public function format(string|float|int|bool|\DateTimeInterface|null $value) : string
    {
        if ($value instanceof \DateTimeInterface) {
            return $value->format(\DateTimeInterface::ATOM);
        }

        if (\is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (null === $value) {
            return $this->nullValue;
        }

        if (\is_numeric($value)) {
            if (\is_int($value)) {
                return \number_format($value, 0);
            }

            return \number_format((float) $value, 2);
        }

        return (string) $value;
    }
}
