<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function pack;
use function strlen;

/**
 * @implements ValueEncoder<\Flow\Types\Value\Json>
 */
final class JsonEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        $json = $value->toString();

        return pack('V', strlen($json)) . $json . ($value->isObject() ? "\x01" : "\x00");
    }
}
