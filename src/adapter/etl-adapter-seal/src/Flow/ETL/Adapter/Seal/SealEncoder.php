<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use DateInterval;
use DateTimeInterface;
use DateTimeZone;
use Dom\Element;
use Dom\XMLDocument;
use DOMDocument;
use DOMElement;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use UnitEnum;

use function array_keys;
use function Flow\ETL\DSL\date_interval_to_microseconds;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;

/**
 * @implements Encoder<array<string, mixed>>
 */
final class SealEncoder implements Encoder
{
    public function __construct(
        private readonly string $dateTimeFormat = DateTimeInterface::ATOM,
        private readonly string $dateFormat = 'Y-m-d',
    ) {}

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
        $documents = [];

        foreach ($batch as $rowValues) {
            $document = [];

            /** @var mixed $value */
            foreach ($rowValues->values as $name => $value) {
                $document[$name] = $this->renderValue($rowValues->types[$name], $value);
            }

            $documents[] = $document;
        }

        return $documents;
    }

    private function renderValue(Type $type, mixed $value): string|float|int|bool|array|null
    {
        if ($value === null) {
            return null;
        }

        return match ($type::class) {
            DateType::class => $value instanceof DateTimeInterface ? $value->format($this->dateFormat) : null,
            DateTimeType::class => $value instanceof DateTimeInterface ? $value->format($this->dateTimeFormat) : null,
            TimeType::class => $value instanceof DateInterval ? date_interval_to_microseconds($value) : null,
            UuidType::class => $value instanceof Uuid ? $value->toString() : null,
            TimeZoneType::class => $value instanceof DateTimeZone ? $value->getName() : null,
            EnumType::class => $value instanceof UnitEnum ? $value->name : null,
            XMLType::class, XMLElementType::class => $this->xmlToString($value),
            JsonType::class => $value instanceof Json ? $this->normalizeArray($value->toArray()) : null,
            ListType::class, MapType::class, StructureType::class, ArrayType::class => $this->normalizeArray($value),
            default => $this->normalizeValue($value),
        };
    }

    /**
     * @return null|array<array-key, mixed>
     */
    private function normalizeArray(mixed $value): ?array
    {
        if (!is_array($value)) {
            return null;
        }

        $normalized = [];

        foreach (array_keys($value) as $key) {
            $normalized[$key] = $this->normalizeValue($value[$key]);
        }

        return $normalized;
    }

    private function normalizeValue(mixed $value): string|float|int|bool|array|null
    {
        if ($value instanceof DateTimeInterface) {
            return $value->format($this->dateTimeFormat);
        }

        if (is_array($value)) {
            return $this->normalizeArray($value);
        }

        if (is_string($value) || is_int($value) || is_float($value) || is_bool($value)) {
            return $value;
        }

        return null;
    }

    private function xmlToString(mixed $value): ?string
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

        return null;
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
