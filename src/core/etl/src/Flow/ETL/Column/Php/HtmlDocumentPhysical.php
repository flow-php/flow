<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use Flow\Floe\ValueDecoder;

use function assert;
use function is_object;
use function is_string;

final readonly class HtmlDocumentPhysical implements Physical
{
    public function toPhysical(mixed $value): mixed
    {
        assert(is_object($value));

        /** @var \Dom\HTMLDocument $value */
        // @mago-ignore analysis:unavailable-method
        return $value->saveHtml();
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        return ValueDecoder::htmlDocumentFromString($physical);
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            assert($physical === null || is_string($physical));
            $values[] = $physical === null ? null : ValueDecoder::htmlDocumentFromString($physical);
        }

        return $values;
    }
}
