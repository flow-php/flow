<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

/**
 * Turns one PHP value into its Floe binary representation. Implementations are
 * built per column type by the {@see \Flow\Floe\ValueEncoder} factory.
 */
interface ValueEncoder
{
    public function encode(mixed $value): string;
}
