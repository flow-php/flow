<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function is_finite;
use function is_float;
use function is_nan;

final readonly class NonFiniteFloat
{
    public static function fromText(string $text): ?float
    {
        return match ($text) {
            'NAN' => NAN,
            'INF' => INF,
            '-INF' => -INF,
            default => null,
        };
    }

    public static function is(mixed $value): bool
    {
        return is_float($value) && !is_finite($value);
    }

    /**
     * @return null|'-INF'|'INF'|'NAN'
     */
    public static function text(float $value): ?string
    {
        return match (true) {
            is_nan($value) => 'NAN',
            $value === INF => 'INF',
            $value === -INF => '-INF',
            default => null,
        };
    }
}
