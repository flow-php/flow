<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use DateInterval;
use DateTimeInterface;
use DateTimeZone;
use Dom\Element;
use Dom\XMLDocument;
use DOMDocument;
use DOMElement;
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
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use Stringable;
use UnitEnum;

use function Flow\ETL\DSL\date_interval_to_microseconds;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_resource;
use function is_string;
use function json_encode;

use const JSON_THROW_ON_ERROR;

final class CSVEncoder
{
    /**
     * @var null|resource
     */
    private $buffer = null;

    public function __construct(
        private readonly string $separator = ',',
        private readonly string $enclosure = '"',
        private readonly string $escape = '\\',
        private readonly string $dateTimeFormat = DateTimeInterface::ATOM,
        private readonly string $dateFormat = 'Y-m-d',
        private readonly string $newLineSeparator = PHP_EOL,
    ) {}

    /**
     * @return list<string>
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

        $lines = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $fields = [];

            foreach ($columns as $name => $column) {
                $fields[] = $this->renderValue($types[$name], $column[$i]);
            }

            $lines[] = $this->line($fields);
        }

        return $lines;
    }

    /**
     * @param list<string> $headers
     */
    public function encodeHeader(array $headers): string
    {
        return $this->line($headers);
    }

    /**
     * @param list<null|bool|float|int|string> $fields
     */
    private function line(array $fields): string
    {
        $buffer = $this->buffer();
        ftruncate($buffer, 0);
        rewind($buffer);

        fputcsv(
            stream: $buffer,
            fields: $fields,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            eol: $this->newLineSeparator,
        );

        $line = stream_get_contents($buffer, offset: 0);

        if ($line === false) {
            throw new RuntimeException('Failed to render a CSV line');
        }

        return $line;
    }

    private function renderValue(Type $type, mixed $value): string|float|int|bool|null
    {
        if ($value === null) {
            return null;
        }

        return match ($type::class) {
            DateTimeType::class => $value instanceof DateTimeInterface ? $value->format($this->dateTimeFormat) : '',
            DateType::class => $value instanceof DateTimeInterface ? $value->format($this->dateFormat) : '',
            TimeType::class => $value instanceof DateInterval ? date_interval_to_microseconds($value) : '',
            EnumType::class => $value instanceof UnitEnum ? $value->name : '',
            JsonType::class => $value instanceof Json ? $value->toString() : '',
            UuidType::class => $value instanceof Uuid ? $value->toString() : '',
            TimeZoneType::class => $value instanceof DateTimeZone ? $value->getName() : '',
            XMLType::class, XMLElementType::class => $this->xmlToString($value),
            ListType::class, MapType::class, StructureType::class, ArrayType::class => is_array($value)
                ? json_encode($value, JSON_THROW_ON_ERROR)
                : '',
            default => $this->scalar($value),
        };
    }

    private function scalar(mixed $value): string|float|int|bool
    {
        return match (true) {
            is_string($value), is_int($value), is_float($value), is_bool($value) => $value,
            is_resource($value), $value instanceof Stringable => (string) $value,
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

    private function domString(string|false $serialized): string
    {
        if ($serialized === false) {
            throw new RuntimeException('Failed to serialize XML document.');
        }

        return $serialized;
    }

    /**
     * @return resource
     */
    private function buffer()
    {
        if (is_resource($this->buffer)) {
            return $this->buffer;
        }

        $buffer = fopen('php://temp/maxmemory:' . (5 * 1024 * 1024), 'rb+');

        if ($buffer === false) {
            throw new RuntimeException('Failed to open a temporary CSV buffer');
        }

        return $this->buffer = $buffer;
    }
}
