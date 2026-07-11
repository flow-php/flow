<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

final class BooleanDecoder implements ValueDecoder
{
    public function decode(string $data, int &$position): bool
    {
        return $data[$position++] === "\x01";
    }
}
