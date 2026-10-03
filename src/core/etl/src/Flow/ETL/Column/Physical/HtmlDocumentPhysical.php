<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use Dom\HTMLDocument;
use Flow\ETL\Exception\InvalidArgumentException;

use function assert;
use function class_exists;
use function is_object;
use function is_string;

use const LIBXML_NOERROR;

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

        if (!class_exists('\Dom\HTMLDocument')) {
            throw new InvalidArgumentException('Floe cannot restore HTML values, \Dom\HTMLDocument requires PHP 8.4+');
        }

        // @mago-expect analysis:unavailable-method
        return HTMLDocument::createFromString($physical, LIBXML_NOERROR);
    }

    public function fromPhysicalAll(array $physicals): array
    {
        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($physicals as $physical) {
            $values[] = $physical === null ? null : $this->fromPhysical($physical);
        }

        return $values;
    }
}
