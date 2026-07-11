<?php

declare(strict_types=1);

namespace Flow\Floe\Encoding;

use function pack;
use function strlen;

final class EnumEncoder implements ValueEncoder
{
    public function encode(mixed $value): string
    {
        /** @var \UnitEnum $value */
        $class = $value::class;

        return pack('V', strlen($class)) . $class . pack('V', strlen($value->name)) . $value->name;
    }
}
