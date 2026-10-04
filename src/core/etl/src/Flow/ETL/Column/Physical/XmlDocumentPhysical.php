<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use DOMDocument;
use Flow\ETL\Exception\InvalidArgumentException;

use function assert;
use function in_array;
use function is_string;
use function sprintf;
use function str_ends_with;
use function str_starts_with;
use function strlen;
use function substr;

final readonly class XmlDocumentPhysical implements Physical
{
    public const string DECLARATION = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

    public function physical(string $outerXml): string
    {
        return self::DECLARATION . $outerXml . "\n";
    }

    /**
     * The outer XML of bytes physical() produced, null for any other bytes: an older declaration, or a comment, PI,
     * DOCTYPE or declaration beside the root.
     */
    public function text(string $physical): ?string
    {
        if (!str_starts_with($physical, self::DECLARATION) || !str_ends_with($physical, ">\n")) {
            return null;
        }

        $outer = substr($physical, strlen(self::DECLARATION), -1);

        return $outer[0] === '<'
        && !in_array($outer[1] ?? '', ['!', '?'], true)
        && !str_ends_with($outer, '-->')
        && !str_ends_with($outer, '?>')
        && !str_ends_with($outer, ']]>')
            ? $outer
            : null;
    }

    public function toPhysical(mixed $value): mixed
    {
        assert($value instanceof DOMDocument);

        $xml = $value->saveXML();

        if ($xml === false) {
            throw new InvalidArgumentException('Floe failed to convert DOMDocument to XML string');
        }

        return (new XmlCharacterReferences())->decoded($xml);
    }

    public function fromPhysical(mixed $physical): mixed
    {
        assert(is_string($physical));

        $document = new DOMDocument();

        if (!@$document->loadXML($physical)) {
            throw new InvalidArgumentException(sprintf('Floe failed to restore DOMDocument from "%s"', $physical));
        }

        // libxml2 before 2.13 writes non-ASCII attribute characters of a document without an encoding as references
        $document->encoding ??= 'UTF-8';

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
