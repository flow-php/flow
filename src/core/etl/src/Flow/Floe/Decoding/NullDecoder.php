<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

final class NullDecoder implements ValueDecoder
{
    public function decode(string $data, int &$position): mixed
    {
        return null;
    }
}
