<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel;

use BackedEnum;
use DateInterval;
use DateTimeInterface;
use DateTimeZone;
use Dom\XMLDocument;
use DOMDocument;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Rows;
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
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use UnitEnum;

use function array_values;
use function is_array;
use function is_scalar;
use function json_encode;

use const JSON_THROW_ON_ERROR;

final class ExcelEncoder
{
    public function __construct(
        private readonly string $timeFormat = '%H:%I:%S',
    ) {}

    /**
     * @return list<array<int, bool|DateTimeInterface|float|int|string|null>>
     */
    public function encode(Rows $rows): array
    {
        $columns = [];
        $types = [];

        foreach ($rows->schema()->definitions() as $definition) {
            $name = $definition->entry()->name();
            $columns[$name] = $rows->column($name)->values();
            $types[$name] = $definition->type();
        }

        $encoded = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $cells = [];

            foreach ($columns as $name => $column) {
                $cells[] = $this->renderValue($types[$name], $column[$i]);
            }

            $encoded[] = $cells;
        }

        return $encoded;
    }

    /**
     * @param list<string> $headers
     *
     * @return list<string>
     */
    public function encodeHeader(array $headers): array
    {
        return array_values($headers);
    }

    private function renderValue(Type $type, mixed $value): bool|DateTimeInterface|float|int|string|null
    {
        if ($value === null) {
            return null;
        }

        return match ($type::class) {
            // a real DateTimeCell, not text: the reader types a cell from its style, and a formatted string is
            // indistinguishable from any other text
            DateTimeType::class, DateType::class => $value instanceof DateTimeInterface ? $value : null,
            TimeType::class => $value instanceof DateInterval ? $value->format($this->timeFormat) : null,
            EnumType::class => match (true) {
                $value instanceof BackedEnum => (string) $value->value,
                $value instanceof UnitEnum => $value->name,
                default => null,
            },
            JsonType::class => $value instanceof Json ? $value->toString() : null,
            UuidType::class => $value instanceof Uuid ? $value->toString() : null,
            TimeZoneType::class => $value instanceof DateTimeZone ? $value->getName() : null,
            XMLType::class => $value instanceof XMLDocument || $value instanceof DOMDocument
                ? $this->xmlToString($value)
                : null,
            ListType::class, MapType::class, StructureType::class, ArrayType::class => is_array($value)
                ? json_encode($value, JSON_THROW_ON_ERROR)
                : null,
            default => $this->scalar($value),
        };
    }

    private function scalar(mixed $value): bool|float|int|string|null
    {
        return match (true) {
            is_scalar($value) => $value,
            default => null,
        };
    }

    private function xmlToString(XMLDocument|DOMDocument $value): string
    {
        $serialized = $value instanceof XMLDocument
            ? $value->saveXml($value->documentElement)
            : $value->saveXML($value->documentElement);

        if ($serialized === false) {
            throw new RuntimeException('Failed to serialize XML document.');
        }

        return $serialized;
    }
}
