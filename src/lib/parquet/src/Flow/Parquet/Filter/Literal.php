<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

use DateTimeImmutable;
use DateTimeInterface;
use InvalidArgumentException;
use Stringable;

use function get_debug_type;
use function is_bool;
use function is_float;
use function is_int;
use function is_nan;
use function is_string;
use function sprintf;

final readonly class Literal
{
    private function __construct() {}

    public static function of(mixed $value): bool|DateTimeImmutable|float|int|string
    {
        return match (true) {
            $value === null => throw new InvalidArgumentException('A filter literal cannot be null, use isNull()'),
            is_float($value) && is_nan($value) => throw new InvalidArgumentException('A filter literal cannot be NaN'),
            is_bool($value), is_float($value), is_int($value), is_string($value) => $value,
            $value instanceof DateTimeInterface => DateTimeImmutable::createFromInterface($value),
            $value instanceof Stringable => (string) $value,
            default => throw new InvalidArgumentException(sprintf(
                'A filter literal must be a scalar, a DateTimeInterface or a Stringable, %s given',
                get_debug_type($value),
            )),
        };
    }
}
