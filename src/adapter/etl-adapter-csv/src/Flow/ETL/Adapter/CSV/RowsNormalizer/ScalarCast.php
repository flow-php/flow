<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\RowsNormalizer;

use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_object;
use function is_resource;
use function is_string;
use function method_exists;

final readonly class ScalarCast
{
    public static function fromMixed(mixed $value): string|float|int|bool|null
    {
        if (is_string($value)) {
            return $value;
        }

        if (is_float($value)) {
            return $value;
        }

        if (is_int($value)) {
            return $value;
        }

        if (is_bool($value)) {
            return $value;
        }

        if ($value === null) {
            return null;
        }

        if (is_resource($value)) {
            return (string) $value;
        }

        if (is_object($value) && method_exists($value, '__toString')) {
            return (string) $value;
        }

        if (is_array($value) || is_object($value)) {
            return '';
        }

        return '';
    }
}
