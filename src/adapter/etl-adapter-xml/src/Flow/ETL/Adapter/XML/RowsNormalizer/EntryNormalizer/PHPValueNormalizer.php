<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML\RowsNormalizer\EntryNormalizer;

use function Flow\ETL\DSL\{type_json, type_string};
use Flow\ETL\Adapter\XML\Abstraction\{XMLAttribute, XMLNode};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\{DateTimeType, JsonType, ListType, MapType, StructureType, UuidType};
use Flow\ETL\PHP\Type\Native\{ArrayType,
    BooleanType,
    EnumType,
    FloatType,
    IntegerType,
    ObjectType,
    StringType};
use Flow\ETL\PHP\Type\{Caster, Type};

final readonly class PHPValueNormalizer
{
    public function __construct(
        private Caster $caster,
        public string $attributePrefix = '_',
        public string $dateTimeFormat = 'Y-m-d\TH:i:s.uP',
        public string $listElementName = 'element',
        public string $mapElementName = 'element',
        public string $mapElementKeyName = 'key',
        public string $mapElementValueName = 'value',
    ) {

    }

    /**
     * @param Type<mixed> $type
     *
     * @throws InvalidArgumentException
     */
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
                $listNode = $listNode->append($this->normalize($this->listElementName, $type->element(), $elementValue));
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
                        ->append($this->normalize($this->mapElementKeyName, $type->key(), $key))
                        ->append($this->normalize($this->mapElementValueName, $type->value(), $elementValue))
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

            foreach ($structureIterator as $keys => $element) {
                /** @var Type<mixed> $structureElementType */
                $structureElementType = $element['structure_element'];
                $structureValue = $element['value_element'];

                $structureNode = $structureNode->append($this->normalize($keys['structure_element'], $structureElementType, $structureValue));
            }

            return $structureNode;
        }

        return match ($type::class) {
            StringType::class,
            IntegerType::class,
            BooleanType::class,
            FloatType::class => XMLNode::flatNode($name, $this->caster->to(type_string())->value($value)),
            ArrayType::class => XMLNode::flatNode($name, $this->caster->to(type_json())->value($value)),
            EnumType::class => XMLNode::flatNode($name, $value->name),
            ObjectType::class => XMLNode::flatNode($name, $this->caster->to(type_string())->value($value)),
            DateTimeType::class => XMLNode::flatNode($name, $this->caster->to(type_string())->value($value->format($this->dateTimeFormat))),
            JsonType::class => XMLNode::flatNode($name, $this->caster->to(type_json())->value($value)),
            UuidType::class => XMLNode::flatNode($name, (string) $value),
            default => throw new InvalidArgumentException("Given type can't be converted to node, given type: {$type->toString()}"),
        };
    }
}
