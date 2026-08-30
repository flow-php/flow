<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function is_string;

final readonly class ArrayKey
{
    /**
     * PHP's canonical array-key coercion: a string whose integer round trip is byte-exact becomes
     * an int ('0' -> 0), everything else stays as-is ('01', '1.5', '-0' stay strings) - the same
     * rule the Rust extension applies in array_key_index().
     */
    public static function coerce(int|string $key): int|string
    {
        if (is_string($key) && (string) (int) $key === $key) {
            return (int) $key;
        }

        return $key;
    }
}
