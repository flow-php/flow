<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Php;

use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;

use function assert;
use function is_string;
use function sprintf;

final readonly class XmlDocumentPhysical implements Physical
{
    public function toPhysical(mixed $value): mixed
    {
        assert($value instanceof DOMDocument);

        $xml = $value->saveXML();

        if ($xml === false) {
            throw new InvalidArgumentException('Floe failed to convert DOMDocument to XML string');
        }

        return $xml;
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        $document = new DOMDocument();

        if (!@$document->loadXML($physical)) {
            throw new InvalidArgumentException(sprintf('Floe failed to restore DOMDocument from "%s"', $physical));
        }

        return $document;
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
