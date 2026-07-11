<?php

declare(strict_types=1);

namespace Flow\Floe\Decoding;

use DOMDocument;
use Flow\Floe\ValueDecoder as ValueDecoderFactory;

use function substr;
use function unpack;

final class XmlDocumentDecoder implements ValueDecoder
{
    public function decode(string $data, int &$position): DOMDocument
    {
        $length = unpack('V', $data, $position)[1];
        $xml = substr($data, $position + 4, $length);
        $position += 4 + $length;

        return ValueDecoderFactory::xmlDocumentFromString($xml);
    }
}
