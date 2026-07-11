<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use function unpack;

final class Int64Decoder implements ValueDecoder
{
    public function decode(string $data, int &$position): int
    {
        $value = unpack('P', $data, $position)[1];
        $position += 8;

        return $value;
    }
}
