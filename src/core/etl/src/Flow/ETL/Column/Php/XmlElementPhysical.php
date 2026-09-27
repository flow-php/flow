<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use DOMElement;
use Flow\Floe\ValueDecoder;
use Flow\Floe\ValueEncoder;

use function assert;
use function is_string;

/**
 * A \Dom\Element (PHP 8.4) is stored by its markup too and reads back as the DOMElement XMLElementType::cast() builds
 * from a string.
 */
final readonly class XmlElementPhysical implements Physical
{
    public function toPhysical(mixed $value): mixed
    {
        if ($value instanceof DOMElement) {
            return ValueEncoder::xmlElementToString($value);
        }

        /** @var \Dom\Element $value */
        // @mago-ignore analysis:non-existent-method
        // @mago-ignore analysis:mixed-assignment
        $markup = $value->ownerDocument?->saveXml($value);
        assert(is_string($markup));

        return $markup;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        return ValueDecoder::xmlElementFromString($physical);
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            assert($physical === null || is_string($physical));
            $values[] = $physical === null ? null : ValueDecoder::xmlElementFromString($physical);
        }

        return $values;
    }
}
