<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use function substr;
use function unpack;

final class StringDecoder implements ValueDecoder
{
    public function decode(string $data, int &$position): string
    {
        $length = unpack('V', $data, $position)[1];
        $value = substr($data, $position + 4, $length);
        $position += 4 + $length;

        return $value;
    }
}
