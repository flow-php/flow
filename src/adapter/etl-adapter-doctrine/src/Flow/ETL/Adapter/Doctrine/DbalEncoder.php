<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Dom\Element;
use Dom\XMLDocument;
use DOMDocument;
use DOMElement;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;

/**
 * @implements Encoder<array<string, mixed>>
 */
final class DbalEncoder implements Encoder
{
    public function decode(array $batch): array
    {
        $decoded = [];

        foreach ($batch as $values) {
            $decoded[] = new RawRowValues($values);
        }

        return $decoded;
    }

    public function encode(array $batch): array
    {
        $normalized = [];

        foreach ($batch as $rowValues) {
            $values = $rowValues->values;
            $row = [];

            /** @var mixed $value */
            foreach ($values as $name => $value) {
                $row[$name] = $this->renderValue($value);
            }

            $normalized[] = $row;
        }

        return $normalized;
    }

    private function renderValue(mixed $value): mixed
    {
        if ($value instanceof XMLDocument) {
            return $this->domString($value->saveXml($value->documentElement));
        }

        if ($value instanceof DOMDocument) {
            return $this->domString($value->saveXML($value->documentElement));
        }

        if ($value instanceof Element || $value instanceof DOMElement) {
            return $this->elementToString($value);
        }

        return $value;
    }

    private function elementToString(Element|DOMElement $value): string
    {
        $ownerDocument = $value->ownerDocument;

        if ($ownerDocument === null) {
            return '';
        }

        if ($ownerDocument instanceof XMLDocument) {
            // @mago-ignore analysis:possibly-invalid-argument
            return $this->domString($ownerDocument->saveXml($value));
        }

        /** @var false|string $serialized */
        // @mago-ignore analysis:possibly-invalid-argument,non-existent-method
        $serialized = $ownerDocument->saveXML($value);

        return $this->domString($serialized);
    }

    private function domString(string|false $serialized): string
    {
        if ($serialized === false) {
            throw new RuntimeException('Failed to serialize XML document.');
        }

        return $serialized;
    }
}
