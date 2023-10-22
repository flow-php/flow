<?php

declare(strict_types=1);

namespace Flow\Parquet;

/**
 * @psalm-suppress MixedArrayOffset
 * @psalm-suppress MixedAssignment
 */
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
