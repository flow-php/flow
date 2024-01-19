<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use function Flow\ETL\DSL\type_array;
use function Flow\ETL\DSL\type_boolean;
use function Flow\ETL\DSL\type_datetime;
use function Flow\ETL\DSL\type_float;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_json;
use function Flow\ETL\DSL\type_null;
use function Flow\ETL\DSL\type_object;
use function Flow\ETL\DSL\type_string;
use function Flow\ETL\DSL\type_uuid;
use function Flow\ETL\DSL\type_xml;
use function Flow\ETL\DSL\type_xml_node;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\EnumType;

final class TypeDetector
{
    public function detectType(mixed $value) : Type
    {
        if (null === $value) {
            return type_null();
        }

        if (\is_string($value)) {
            if (type_json()->isValid($value)) {
                return type_json();
            }

            return type_string();
        }

        if (\is_int($value)) {
            return type_int();
        }

        if (\is_bool($value)) {
            return type_boolean();
        }

        if (\is_float($value)) {
            return type_float();
        }

        if (\is_array($value)) {
            if ([] === $value) {
                return ArrayType::empty();
            }

            $detector = new ArrayContentDetector(
                new Types(...\array_map([$this, 'detectType'], \array_keys($value))),
                new Types(...\array_map([$this, 'detectType'], \array_values($value)))
            );

            $firstValue = $detector->firstValueType();

            if ($detector->isList() && $firstValue) {
                return new ListType(ListElement::fromType($firstValue));
            }

            if ($detector->isStructure() || $detector->isMap()) {
                $elements = [];

                foreach ($value as $key => $item) {
                    $elements[] = new StructureElement($key, $this->detectType($item));
                }

                return new StructureType($elements);
            }

            return type_array([] === \array_filter($value, fn ($value) : bool => null !== $value));
        }

        if ($value instanceof \UnitEnum) {
            return EnumType::of($value::class);
        }

        if (\is_object($value)) {
            if (type_uuid()->isValid($value)) {
                return type_uuid();
            }

            if (type_datetime()->isValid($value)) {
                return type_datetime();
            }

            if (type_xml()->isValid($value)) {
                return type_xml();
            }

            if (type_xml_node()->isValid($value)) {
                return type_xml_node();
            }

            return type_object($value::class);
        }

        throw InvalidArgumentException::because('Unsupported type given: ' . \gettype($value));
    }
}
