<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

/**
 * @implements ValueEncoder<null>
 */
final class NullEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        return '';
    }
}
