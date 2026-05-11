<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer;

use Flow\ETL\Adapter\XML\Abstraction\XMLAttribute;
use Flow\ETL\Adapter\XML\Abstraction\XMLNode;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\InstanceOfType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function Flow\Types\DSL\type_string;

final readonly class PHPValueNormalizer
{
    public function __construct(
        public string $attributePrefix = '_',
        public string $dateTimeFormat = 'Y-m-d\TH:i:s.uP',
        public string $listElementName = 'element',
        public string $mapElementName = 'element',
        public string $mapElementKeyName = 'key',
        public string $mapElementValueName = 'value',
    ) {}

    /**
     * @param Type<mixed> $type
     *
     * @throws InvalidArgumentException
     */
    public function normalize(string $name, Type $type, mixed $value): XMLNode|XMLAttribute
    {
        if (\str_starts_with($name, $this->attributePrefix)) {
            return new XMLAttribute(
                \substr($name, \strlen($this->attributePrefix)),
                (string) type_string()->cast($value),
            );
        }

        if ($value === null) {
            return XMLNode::flatNode($name, '');
        }

        if ($type instanceof ListType) {
            $listNode = XMLNode::nestedNode($name);

            if (!\is_array($value) && !$value instanceof \Countable) {
                return $listNode;
            }

            if (!\count($value)) {
                return $listNode;
            }

            if (!\is_iterable($value)) {
                return $listNode;
            }

            foreach ($value as $elementValue) {
                $listNode = $listNode->append($this->normalize(
                    $this->listElementName,
                    $type->element(),
                    $elementValue,
                ));
            }

            return $listNode;
        }

        if ($type instanceof MapType) {
            $mapNode = XMLNode::nestedNode($name);

            if (!\is_array($value) && !$value instanceof \Countable) {
                return $mapNode;
            }

            if (!\count($value)) {
                return $mapNode;
            }

            if (!\is_iterable($value)) {
                return $mapNode;
            }

            foreach ($value as $key => $elementValue) {
                $mapNode = $mapNode->append(
                    XMLNode::nestedNode($this->mapElementName)
                        ->append($this->normalize($this->mapElementKeyName, $type->key(), $key))
                        ->append($this->normalize($this->mapElementValueName, $type->value(), $elementValue)),
                );
            }

            return $mapNode;
        }

        if ($type instanceof StructureType) {
            $structureNode = XMLNode::nestedNode($name);

            if (!\count($type->elements())) {
                return $structureNode;
            }

            $structureIterator = new \MultipleIterator(\MultipleIterator::MIT_KEYS_ASSOC);
            $structureIterator->attachIterator(new \ArrayIterator($type->elements()), 'structure_element');
            $structureIterator->attachIterator(new \ArrayIterator(\is_array($value) ? $value : []), 'value_element');

            foreach ($structureIterator as $keys => $element) {
                /** @var Type<mixed> $structureElementType */
                $structureElementType = $element['structure_element'];
                $structureValue = $element['value_element'];

                $structureNode = $structureNode->append($this->normalize(
                    $keys['structure_element'],
                    $structureElementType,
                    $structureValue,
                ));
            }

            return $structureNode;
        }

        return match ($type::class) {
            StringType::class, IntegerType::class, BooleanType::class, FloatType::class => XMLNode::flatNode(
                $name,
                type_string()->cast($value),
            ),
            ArrayType::class => XMLNode::flatNode(
                $name,
                \is_array($value) ? \json_encode($value, \JSON_THROW_ON_ERROR) : '',
            ),
            EnumType::class => XMLNode::flatNode($name, $value instanceof \BackedEnum ? $value->name : ''),
            InstanceOfType::class => XMLNode::flatNode($name, type_string()->cast($value)),
            DateTimeType::class => XMLNode::flatNode(
                $name,
                type_string()->cast($value instanceof \DateTimeInterface ? $value->format($this->dateTimeFormat) : ''),
            ),
            JsonType::class => XMLNode::flatNode($name, $value instanceof \Stringable ? $value->__toString() : ''),
            UuidType::class => XMLNode::flatNode(
                $name,
                \is_scalar($value) || $value instanceof \Stringable ? (string) $value : '',
            ),
            default => throw new InvalidArgumentException(
                "Given type can't be converted to node, given type: {$type->toString()}",
            ),
        };
    }
}
