<?php

declare(strict_types=1);

namespace Flow\ETL\Serializer;

use Dom\HTMLDocument;
use DOMDocument;
use DOMElement;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;

use function base64_decode;
use function base64_encode;
use function Flow\Types\DSL\type_string;
use function gzcompress;
use function gzuncompress;
use function sprintf;

/**
 * Executed on PHP 8.4.22: serialize() throws for DOMDocument, DOMElement, Dom\HTMLDocument and
 * Dom\HTMLElement alike, so every DOM-backed column travels as gzcompressed, base64 encoded markup.
 * Every other column is native.
 */
final readonly class DomValueCodec
{
    /**
     * @param Type<mixed> $type
     */
    public function decode(Type $type, mixed $value): mixed
    {
        if (!$this->handles($type) || $value === null) {
            return $value;
        }

        $decoded = base64_decode(type_string()->assert($value), true);

        if ($decoded === false) {
            throw new InvalidArgumentException('Given value is not valid base64');
        }

        $xml = @gzuncompress($decoded);

        if ($xml === false) {
            throw new InvalidArgumentException('Given value is not valid gzcompressed XML');
        }

        if ($type instanceof HTMLType) {
            // @mago-expect analysis:unavailable-method
            return HTMLDocument::createFromString($xml, LIBXML_NOERROR);
        }

        if ($type instanceof HTMLElementType) {
            // @mago-expect analysis:unavailable-method
            $html = HTMLDocument::createFromString($xml, LIBXML_NOERROR);
            $element = $html->documentElement;

            if ($element === null) {
                throw new InvalidArgumentException('Given HTML does not contain a document element');
            }

            return $element;
        }

        $document = new DOMDocument();

        if (!@$document->loadXML($xml)) {
            throw new InvalidArgumentException(sprintf('Given string "%s" is not valid XML', $xml));
        }

        if ($type instanceof XMLType) {
            return $document;
        }

        $documentElement = $document->documentElement;

        if ($documentElement === null) {
            throw new InvalidArgumentException('Given XML does not contain a document element');
        }

        $target = new DOMDocument();
        $imported = $target->importNode($documentElement, true);

        if (!$imported instanceof DOMElement) {
            throw new InvalidArgumentException('Imported node is not a DOMElement');
        }

        // an unparented node canonicalizes to "", which would make every restored element compare equal
        $target->appendChild($imported);

        return $imported;
    }

    /**
     * @param Type<mixed> $type
     */
    public function encode(Type $type, mixed $value): mixed
    {
        if (!$this->handles($type) || $value === null) {
            return $value;
        }

        $compressed = gzcompress(type_string()->cast($value));

        if ($compressed === false) {
            throw new RuntimeException('Failed to gzcompress XML column value.');
        }

        return base64_encode($compressed);
    }

    /**
     * @param Type<mixed> $type
     */
    public function handles(Type $type): bool
    {
        return (
            $type instanceof XMLType
            || $type instanceof XMLElementType
            || $type instanceof HTMLType
            || $type instanceof HTMLElementType
        );
    }
}
