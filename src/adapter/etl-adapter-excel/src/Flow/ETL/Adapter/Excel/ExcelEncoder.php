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
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Stringable;
use UnitEnum;

use function array_map;
use function array_values;
use function count;
use function is_array;
use function is_scalar;
use function json_encode;
use function str_pad;

use const JSON_THROW_ON_ERROR;

/**
 * @implements Encoder<array<int, mixed>>
 */
final class ExcelEncoder implements Encoder
{
    /**
     * @var null|list<string>
     */
    private ?array $headers = null;

    public function __construct(
        private readonly bool $withHeader = true,
        private readonly bool $convertEmptyToNull = true,
        private readonly string $dateTimeFormat = 'Y-m-d H:i:s',
        private readonly string $dateFormat = 'Y-m-d',
        private readonly string $timeFormat = '%H:%I:%S',
    ) {}

    public function decode(array $batch): array
    {
        $decoded = [];

        foreach ($batch as $cells) {
            if ($this->headers === null) {
                if ($this->withHeader) {
                    $this->headers = $this->mapHeaders($cells);

                    continue;
                }

                $this->headers = $this->generateAutoHeaders(count($cells));
            }

            $values = [];

            foreach ($this->headers as $index => $name) {
                // @mago-ignore analysis:mixed-assignment
                $cell = $cells[$index] ?? null;
                $values[$name] = $this->convertEmptyToNull && '' === $cell ? null : $cell;
            }

            $decoded[] = new RawRowValues($values);
        }

        return $decoded;
    }

    public function encode(array $batch): array
    {
        $rows = [];

        foreach ($batch as $rowValues) {
            $cells = [];

            /** @var mixed $value */
            foreach ($rowValues->values as $name => $value) {
                $cells[] = $this->renderValue($rowValues->types[$name], $value);
            }

            $rows[] = $cells;
        }

        return $rows;
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

    /**
     * @return list<string>
     */
    private function generateAutoHeaders(int $count): array
    {
        $headers = [];

        for ($i = 0; $i < $count; $i++) {
            $headers[] = 'e' . str_pad((string) $i, 2, '0', STR_PAD_LEFT);
        }

        return $headers;
    }

    /**
     * @param array<int, mixed> $cells
     *
     * @return list<string>
     */
    private function mapHeaders(array $cells): array
    {
        return array_values(array_map(static fn(mixed $header): string => is_scalar($header)
            ? (string) $header
            : '', $cells));
    }

    private function renderValue(Type $type, mixed $value): bool|float|int|string|null
    {
        if ($value === null) {
            return null;
        }

        return match ($type::class) {
            DateTimeType::class => $value instanceof DateTimeInterface ? $value->format($this->dateTimeFormat) : null,
            DateType::class => $value instanceof DateTimeInterface ? $value->format($this->dateFormat) : null,
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
            $value instanceof Stringable => (string) $value,
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
