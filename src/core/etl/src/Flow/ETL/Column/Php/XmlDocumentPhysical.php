<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use DOMDocument;
use Flow\Floe\ValueDecoder;
use Flow\Floe\ValueEncoder;

use function assert;
use function is_string;

final readonly class XmlDocumentPhysical implements Physical
{
    public function toPhysical(mixed $value): mixed
    {
        assert($value instanceof DOMDocument);

        return ValueEncoder::xmlDocumentToString($value);
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        return ValueDecoder::xmlDocumentFromString($physical);
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            assert($physical === null || is_string($physical));
            $values[] = $physical === null ? null : ValueDecoder::xmlDocumentFromString($physical);
        }

        return $values;
    }
}
