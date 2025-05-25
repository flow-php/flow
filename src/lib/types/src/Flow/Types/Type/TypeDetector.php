<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function Flow\Types\DSL\{type_array, type_boolean, type_date, type_datetime, type_enum, type_float, type_instance_of, type_integer, type_json, type_map, type_null, type_string, type_time, type_union, type_uuid, type_xml, type_xml_element, types};
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\{ListType, StructureType};
use Flow\Types\Type\Native\{IntegerType, StringType};

final class TypeDetector
{
    /**
     * @return Type<mixed>
     */
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
            return type_integer();
        }

        if (\is_bool($value)) {
            return type_boolean();
        }

        if (\is_float($value)) {
            return type_float();
        }

        if (\is_array($value)) {
            if ([] === $value) {
                return type_array();
            }

            $detector = new ArrayContentDetector(
                $keyTypes = types(...\array_map($this->detectType(...), \array_keys($value)))->deduplicate(),
                $valueTypes = types(...\array_map($this->detectType(...), \array_values($value)))->deduplicate(),
                \array_is_list($value)
            );

            if ($detector->isList()) {
                return new ListType($detector->valueType());
            }

            if ($detector->isMap()) {
                return type_map(type_union(type_instance_of(StringType::class), type_instance_of(IntegerType::class))->assert($detector->firstKeyType()), $detector->valueType());
            }

            if ($detector->isStructure()) {
                /** @var array<Type<mixed>> $elements */
                $elements = [];

                foreach ($value as $key => $item) {
                    $elements[$key] = $this->detectType($item);
                }

                return new StructureType($elements);
            }

            return type_array();
        }

        if ($value instanceof \UnitEnum) {
            return type_enum($value::class);
        }

        if (\is_object($value)) {
            if (type_uuid()->isValid($value)) {
                return type_uuid();
            }

            if (type_time()->isValid($value)) {
                return type_time();
            }

            if (type_date()->isValid($value)) {
                return type_date();
            }

            if (type_datetime()->isValid($value)) {
                return type_datetime();
            }

            if (type_xml()->isValid($value)) {
                return type_xml();
            }

            if (type_xml_element()->isValid($value)) {
                return type_xml_element();
            }

            return type_instance_of($value::class);
        }

        throw new InvalidArgumentException('Unsupported type given: ' . \gettype($value));
    }
}
