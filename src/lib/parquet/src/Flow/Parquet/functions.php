<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\Dremel\ColumnData\NullLevel;
use Flow\Parquet\Exception\InvalidArgumentException;
use Generator;

use function get_debug_type;
use function is_array;
use function is_int;
use function is_string;

/**
 * @param array<array-key, mixed> $array1
 * @param array<array-key, mixed> $array2
 *
 * @return array<array-key, mixed>
 */
function array_merge_recursive(array $array1, array $array2): array
{
    $merged = $array1;

    // @mago-ignore analysis:mixed-assignment
    foreach ($array2 as $key => &$value) {
        if (is_array($value) && isset($merged[$key]) && is_array($merged[$key])) {
            $merged[$key] = array_merge_recursive($merged[$key], $value);
        } else {
            $merged[$key] = $value;
        }
    }

    return $merged;
}

/**
 * @param array<array-key, mixed> $keys
 * @param array<array-key, mixed> $values
 *
 * @return array<array-key, mixed>
 */
function dremel_array_combine_recursive(array $keys, array $values): array
{
    $result = [];

    // @mago-ignore analysis:mixed-assignment
    foreach ($keys as $keyIndex => $keyValue) {
        // @mago-ignore analysis:mixed-assignment
        $value = $values[$keyIndex] ?? null;

        if ($keyValue === null && $value !== null) {
            continue;
        }

        if ($keyValue instanceof NullLevel && !$value instanceof NullLevel) {
            continue;
        }

        if ($keyValue === null) {
            $result[] = null;

            continue;
        }

        if ($keyValue instanceof NullLevel && $value instanceof NullLevel) {
            $result[] = $value;

            continue;
        }

        if (is_array($keyValue) && is_array($value)) {
            $result[] = dremel_array_combine_recursive($keyValue, $value);
        } else {
            if (!is_int($keyValue) && !is_string($keyValue)) {
                throw new InvalidArgumentException('Map key must be int or string, got ' . get_debug_type($keyValue));
            }
            $result[$keyValue] = $value;
        }
    }

    return $result;
}

/**
 * Iterate over array at given level.
 *
 * @param array<array-key, mixed> $array
 */
function array_iterate_at_level(array &$array, int $targetLevel, callable $callback, int $currentLevel = 1): void
{
    if ($currentLevel === $targetLevel) {
        // @mago-ignore analysis:mixed-assignment
        foreach ($array as &$value) {
            $callback($value);
        }
    } else {
        // @mago-ignore analysis:mixed-assignment
        foreach ($array as &$value) {
            if (is_array($value)) {
                array_iterate_at_level($value, $targetLevel, $callback, $currentLevel + 1);
            }
        }
    }
}

/**
 * @param array<mixed> $array
 *
 * @return array<mixed>
 */
function array_flatten(array $array): array
{
    $result = [];

    $flatten = static function (array $arr) use (&$result, &$flatten): void {
        // @mago-ignore analysis:mixed-assignment
        foreach ($arr as $item) {
            if (is_array($item)) {
                $flatten($item);
            } else {
                $result[] = $item;
            }
        }
    };

    $flatten($array);

    return $result;
}

function empty_generator(): Generator
{
    yield from [];
}
