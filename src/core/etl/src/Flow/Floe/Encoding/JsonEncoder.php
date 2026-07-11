<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function pack;
use function strlen;

final class JsonEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        /** @var \Flow\Types\Value\Json $value */
        $json = $value->toString();

        return pack('V', strlen($json)) . $json . ($value->isObject() ? "\x01" : "\x00");
    }
}
