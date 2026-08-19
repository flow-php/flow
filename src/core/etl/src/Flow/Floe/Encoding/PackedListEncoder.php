<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function count;
use function pack;

/**
 * Fixed-width element lists (int64 'P', float64 'e') packed in one call instead
 * of per element.
 *
 * @implements ValueEncoder<list<float|int>>
 */
final class PackedListEncoder implements ValueEncoder
{
    /**
     * @param 'e'|'P' $format
     */
    public function __construct(
        private readonly string $format,
    ) {}

    public function encode(mixed $value): string
    {
        return count($value) === 0 ? pack('V', 0) : pack('V', count($value)) . pack($this->format . '*', ...$value);
    }
}
