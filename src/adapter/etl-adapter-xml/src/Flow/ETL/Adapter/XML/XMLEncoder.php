<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use BackedEnum;
use Countable;
use DateInterval;
use DateTimeInterface;
use DateTimeZone;
use Dom\XMLDocument;
use DOMDocument;
use Flow\ETL\Adapter\XML\Abstraction\XMLAttribute;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Encoder;
use Flow\ETL\Row\RawRowValues;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\InstanceOfType;
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
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use Stringable;

use function array_values;
use function count;
use function Flow\ETL\DSL\date_interval_to_microseconds;
use function Flow\Types\DSL\type_string;
use function is_array;
use function is_iterable;
use function is_scalar;
use function json_encode;
use function str_starts_with;
use function strlen;
use function substr;

use const JSON_THROW_ON_ERROR;

/**
 * @implements Encoder<string>
 */
final class XMLEncoder implements Encoder
{
    public function __construct(
        private readonly ?XMLWriter $xmlWriter = null,
        private readonly string $attributePrefix = '_',
        private readonly string $dateTimeFormat = 'Y-m-d\TH:i:s.uP',
        private readonly string $dateFormat = 'Y-m-d',
        private readonly string $listElementName = 'element',
        private readonly string $mapElementName = 'element',
        private readonly string $mapElementKeyName = 'key',
        private readonly string $mapElementValueName = 'value',
        private readonly string $rowElementName = 'row',
    ) {}

    public function decode(array $batch): array
    {
        $maps = [];

        foreach ($batch as $xml) {
            $maps[] = new RawRowValues(['node' => $xml]);
        }

        return $maps;
    }

    public function encode(array $batch): array
    {
        $xmlWriter = $this->xmlWriter ?? throw new RuntimeException('XMLEncoder requires an XMLWriter to encode rows');

        $lines = [];

        foreach ($batch as $rowValues) {
            $elements = [];

            foreach ($rowValues->types as $name => $type) {
                $elements[] = $this->normalize($name, $type, $rowValues->values[$name]);
            }

            $lines[] = $xmlWriter->write(XMLNode::nested($this->rowElementName, ...$elements));
        }

        return $lines;
    }

    /**
     * @param Type<mixed> $type
     *
     * @throws InvalidArgumentException
     */
    private function normalize(string $name, Type $type, mixed $value): XMLNode|XMLAttribute
    {
        if ($type instanceof StructureType) {
            foreach ($type->elements() as $element) {
                if ($element->optional) {
                    throw new RuntimeException(sprintf(
                        'XML encoder does not support structure optional elements, given: %s',
                        $type->toString(),
                    ));
                }
            }
        }

        if (str_starts_with($name, $this->attributePrefix)) {
            return new XMLAttribute(substr($name, strlen($this->attributePrefix)), type_string()->cast($value));
        }

        if ($value === null) {
            return XMLNode::flatNode($name, '');
        }

        if ($type instanceof ListType) {
            if (!is_array($value) && !$value instanceof Countable || !count($value) || !is_iterable($value)) {
                return XMLNode::nestedNode($name);
            }

            $elements = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($value as $elementValue) {
                $elements[] = $this->normalize($this->listElementName, $type->element(), $elementValue);
            }

            return XMLNode::nested($name, ...$elements);
        }

        if ($type instanceof MapType) {
            if (!is_array($value) && !$value instanceof Countable || !count($value) || !is_iterable($value)) {
                return XMLNode::nestedNode($name);
            }

            $elements = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($value as $key => $elementValue) {
                $elements[] = XMLNode::nested(
                    $this->mapElementName,
                    $this->normalize($this->mapElementKeyName, $type->key(), $key),
                    $this->normalize($this->mapElementValueName, $type->value(), $elementValue),
                );
            }

            return XMLNode::nested($name, ...$elements);
        }

        if ($type instanceof StructureType) {
            $values = is_array($value) ? array_values($value) : [];

            if (count($values) > count($type->elements())) {
                throw new RuntimeException(sprintf(
                    'XML encoder received %d values for structure "%s" which declares %d elements - extra values would be silently lost',
                    count($values),
                    $type->toString(),
                    count($type->elements()),
                ));
            }

            $elements = [];

            foreach ($type->elements() as $position => $element) {
                $elements[] = $this->normalize((string) $element->name, $element->type, $values[$position] ?? null);
            }

            return XMLNode::nested($name, ...$elements);
        }

        return match ($type::class) {
            StringType::class, IntegerType::class, BooleanType::class, FloatType::class => XMLNode::flatNode(
                $name,
                type_string()->cast($value),
            ),
            ArrayType::class => XMLNode::flatNode(
                $name,
                is_array($value) ? json_encode($value, JSON_THROW_ON_ERROR) : '',
            ),
            EnumType::class => XMLNode::flatNode($name, $value instanceof BackedEnum ? $value->name : ''),
            InstanceOfType::class => XMLNode::flatNode($name, type_string()->cast($value)),
            DateTimeType::class => XMLNode::flatNode(
                $name,
                type_string()->cast($value instanceof DateTimeInterface ? $value->format($this->dateTimeFormat) : ''),
            ),
            DateType::class => XMLNode::flatNode(
                $name,
                type_string()->cast($value instanceof DateTimeInterface ? $value->format($this->dateFormat) : ''),
            ),
            TimeType::class => XMLNode::flatNode(
                $name,
                $value instanceof DateInterval ? (string) date_interval_to_microseconds($value) : '',
            ),
            JsonType::class => XMLNode::flatNode($name, $value instanceof Stringable ? $value->__toString() : ''),
            UuidType::class => XMLNode::flatNode(
                $name,
                is_scalar($value) || $value instanceof Stringable ? (string) $value : '',
            ),
            TimeZoneType::class => XMLNode::flatNode($name, $value instanceof DateTimeZone ? $value->getName() : ''),
            XMLType::class, XMLElementType::class => XMLNode::flatNode($name, $this->xmlToString($value)),
            default => throw new InvalidArgumentException(
                "Given type can't be converted to node, given type: {$type->toString()}",
            ),
        };
    }

    private function xmlToString(mixed $value): string
    {
        if ($value instanceof XMLDocument) {
            $serialized = $value->saveXml($value->documentElement);
        } elseif ($value instanceof DOMDocument) {
            $serialized = $value->saveXML($value->documentElement);
        } elseif ($value instanceof Stringable || is_scalar($value)) {
            return (string) $value;
        } else {
            return '';
        }

        if ($serialized === false) {
            throw new RuntimeException('Failed to serialize XML document.');
        }

        return $serialized;
    }
}
