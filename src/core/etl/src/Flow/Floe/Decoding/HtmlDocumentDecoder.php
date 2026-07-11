<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use Flow\Floe\ValueDecoder as ValueDecoderFactory;

use function substr;
use function unpack;

final class HtmlDocumentDecoder implements ValueDecoder
{
    public function decode(string $data, int &$position): object
    {
        $length = unpack('V', $data, $position)[1];
        $html = substr($data, $position + 4, $length);
        $position += 4 + $length;

        return ValueDecoderFactory::htmlDocumentFromString($html);
    }
}
