<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function pack;
use function strlen;

/**
 * @implements ValueEncoder<\DateTimeZone>
 */
final class TimeZoneEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        $name = $value->getName();

        return pack('V', strlen($name)) . $name;
    }
}
