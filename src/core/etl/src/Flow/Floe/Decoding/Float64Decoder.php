<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use function unpack;

final class Float64Decoder implements ValueDecoder
{
    public function decode(string $data, int &$position): float
    {
        $value = unpack('e', $data, $position)[1];
        $position += 8;

        return $value;
    }
}
