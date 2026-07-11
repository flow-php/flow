<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function chr;
use function pack;

final class IntervalEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        /** @var \DateInterval $value */
        return (
            chr($value->invert)
            . pack('VVVVVV', $value->y, $value->m, $value->d, $value->h, $value->i, $value->s)
            . pack('e', $value->f)
        );
    }
}
