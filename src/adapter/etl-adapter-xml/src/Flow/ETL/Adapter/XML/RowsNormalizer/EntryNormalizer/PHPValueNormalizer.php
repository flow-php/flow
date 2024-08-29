<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer;

use function Flow\ETL\DSL\{type_json, type_string};
use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\{DateTimeType, JsonType, ListType, MapType, StructureType, UuidType};
use Flow\ETL\PHP\Type\Native\{ArrayType, EnumType, ObjectType, ScalarType};
use Flow\ETL\PHP\Type\{Caster, Type};

final class PHPValueNormalizer
{
    public const DATE_TIME_FORMAT = 'Y-m-d\TH:i:s.uP';

    public function __construct(
        private readonly Caster $caster,
        public readonly string $attributePrefix = '_',
        public readonly string $dateTimeFormat = self::DATE_TIME_FORMAT,
        public readonly string $listElementName = 'element',
        public readonly string $mapElementName = 'element',
        public readonly string $mapElementKeyName = 'key',
        public readonly string $mapElementValueName = 'value',
    ) {

    }

    public function normalize(string $name, Type $type, mixed $value) : XMLNode|XMLAttribute
    {
        if (\str_starts_with($name, $this->attributePrefix)) {
            return new XMLAttribute(\substr($name, \strlen($this->attributePrefix)), $this->caster->to(type_string())->value($value));
        }

        if ($value === null) {
            return XMLNode::flatNode($name, '');
        }

        if ($type instanceof ListType) {
            $listNode = XMLNode::nestedNode($name);

            if (!\count($value)) {
                return $listNode;
            }

            foreach ($value as $elementValue) {
                $listNode = $listNode->append($this->normalize($this->listElementName, $type->element()->type(), $elementValue));
            }

            return $listNode;
        }

        if ($type instanceof MapType) {
            $mapNode = XMLNode::nestedNode($name);

            if (!\count($value)) {
                return $mapNode;
            }

            foreach ($value as $key => $elementValue) {
                $mapNode = $mapNode->append(
                    XMLNode::nestedNode($this->mapElementName)
                        ->append($this->normalize($this->mapElementKeyName, $type->key()->type(), $key))
                        ->append($this->normalize($this->mapElementValueName, $type->value()->type(), $elementValue))
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
            $structureIterator->attachIterator(new \ArrayIterator($value), 'value_element');

            foreach ($structureIterator as $element) {
                /** @var StructureElement $structureElement */
                $structureElement = $element['structure_element'];
                $structureValue = $element['value_element'];

                $structureNode = $structureNode->append($this->normalize($structureElement->name(), $structureElement->type(), $structureValue));
            }

            return $structureNode;
        }

        return match ($type::class) {
            ScalarType::class => XMLNode::flatNode($name, $this->caster->to(type_string())->value($value)),
            ArrayType::class => XMLNode::flatNode($name, $this->caster->to(type_json())->value($value)),
            EnumType::class => XMLNode::flatNode($name, $value->name),
            ObjectType::class => XMLNode::flatNode($name, $this->caster->to(type_string())->value($value)),
            DateTimeType::class => XMLNode::flatNode($name, $this->caster->to(type_string())->value($value->format($this->dateTimeFormat))),
            JsonType::class => XMLNode::flatNode($name, $this->caster->to(type_json())->value($value)),
            UuidType::class => XMLNode::flatNode($name, (string) $value),
            default => throw new InvalidArgumentException("Given type can't be converted to node, given type: {$type->toString()}")
        };
    }
}
