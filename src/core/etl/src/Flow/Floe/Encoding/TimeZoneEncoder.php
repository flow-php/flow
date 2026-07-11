<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function pack;
use function strlen;

final class TimeZoneEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        /** @var \DateTimeZone $value */
        $name = $value->getName();

        return pack('V', strlen($name)) . $name;
    }
}
