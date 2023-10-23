<?php

declare(strict_types=1);

namespace Flow\Parquet;

function array_merge_recursive(array $array1, array $array2) : array
{
    $merged = $array1;

    foreach ($array2 as $key => &$value) {
        if (\is_array($value) && isset($merged[$key]) && \is_array($merged[$key])) {
            $merged[$key] = array_merge_recursive($merged[$key], $value);
        } else {
            $merged[$key] = $value;
        }
    }

    return $merged;
}

function array_combine_recursive(array $keys, array $values) : array
{
    $result = [];

    foreach ($keys as $keyIndex => $keyValue) {
        $value = $values[$keyIndex] ?? null;

        if (\is_array($keyValue) && \is_array($value)) {
            $result[] = array_combine_recursive($keyValue, $value);
        } else {
            $result[$keyValue] = $value;
        }
    }

    return $result;
}
