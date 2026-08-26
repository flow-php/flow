<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

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
use Stringable;
use UnitEnum;

use function array_combine;
use function array_keys;
use function array_map;
use function array_values;
use function Flow\ETL\DSL\date_interval_to_microseconds;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;

/**
 * @implements Encoder<array<string, mixed>>
 */
final class JSONEncoder implements Encoder
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
        $encoded = [];

        foreach ($batch as $rowValues) {
            $row = [];

            /** @var mixed $value */
            foreach ($rowValues->values as $name => $value) {
                $row[$name] = $this->renderValue($rowValues->types[$name], $value);
            }

            $encoded[] = $row;
        }

        return $encoded;
    }

    /**
     * @param Type<mixed> $type
     *
     * @return null|array<array-key, mixed>|bool|float|int|string
     */
    private function renderValue(Type $type, mixed $value): string|float|int|bool|array|null
    {
        if ($value === null) {
            return null;
        }

        return match ($type::class) {
            DateTimeType::class => $value instanceof DateTimeInterface ? $value->format($this->dateTimeFormat) : '',
            DateType::class => $value instanceof DateTimeInterface ? $value->format($this->dateFormat) : '',
            TimeType::class => $value instanceof DateInterval ? date_interval_to_microseconds($value) : '',
            EnumType::class => $value instanceof UnitEnum ? $value->name : '',
            JsonType::class => $value instanceof Json ? $this->normalizeJsonValue($value->toArray()) : null,
            UuidType::class => $value instanceof Uuid ? $value->toString() : '',
            TimeZoneType::class => $value instanceof DateTimeZone ? $value->getName() : '',
            XMLType::class, XMLElementType::class => $this->xmlToString($value),
            ListType::class, MapType::class, StructureType::class, ArrayType::class => is_array($value)
                ? $this->normalizeArray($value)
                : null,
            default => $this->scalar($value),
        };
    }

    private function scalar(mixed $value): string|float|int|bool
    {
        return match (true) {
            is_string($value), is_int($value), is_float($value), is_bool($value) => $value,
            $value instanceof Stringable => (string) $value,
            default => '',
        };
    }

    private function xmlToString(mixed $value): string
    {
        return match (true) {
            $value instanceof DOMDocument => $this->domString($value->saveXML($value->documentElement)),
            $value instanceof XMLDocument => $this->domString($value->saveXml($value->documentElement)),
            $value instanceof DOMElement => $this->domString($value->C14N()),
            $value instanceof Element => $this->domString($value->c14n()),
            default => '',
        };
    }

    /**
     * @param array<array-key, mixed> $value
     *
     * @return array<array-key, mixed>
     */
    private function normalizeArray(array $value): array
    {
        $normalized = [];

        foreach (array_keys($value) as $key) {
            $normalized[$key] = $this->normalizeValue($value[$key]);
        }

        return $normalized;
    }

    private function normalizeValue(mixed $value): string|float|int|bool|array|null
    {
        return match (true) {
            $value instanceof DateTimeInterface => $value->format($this->dateTimeFormat),
            $value instanceof DateInterval => date_interval_to_microseconds($value),
            $value instanceof Uuid => $value->toString(),
            $value instanceof Json => $this->normalizeArray($value->toArray()),
            $value instanceof UnitEnum => $value->name,
            is_array($value) => $this->normalizeArray($value),
            is_string($value), is_int($value), is_float($value), is_bool($value) => $value,
            default => null,
        };
    }

    /**
     * @param array<array-key, mixed> $value
     *
     * @return array<string, mixed>
     */
    private function normalizeJsonValue(array $value): array
    {
        return array_combine(
            array_map(static fn(int|string $key): string => (string) $key, array_keys($value)),
            array_values($value),
        );
    }

    private function domString(string|false $serialized): string
    {
        if ($serialized === false) {
            throw new RuntimeException('Failed to serialize XML document.');
        }

        return $serialized;
    }
}
