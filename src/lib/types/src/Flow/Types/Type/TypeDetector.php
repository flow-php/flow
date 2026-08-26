<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use Flow\Types\Value\Uuid;
use UnitEnum;

use function array_is_list;
use function array_keys;
use function array_map;
use function array_values;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;
use function Flow\Types\DSL\types;
use function gettype;
use function is_a;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_object;
use function is_string;

final class TypeDetector
{
    /**
     * @return Type<mixed>
     */
    public function detectType(mixed $value): Type
    {
        if (null === $value) {
            return type_null();
        }

        if ($value instanceof Json) {
            return type_json();
        }

        if ($value instanceof Uuid) {
            return type_uuid();
        }

        if (is_string($value)) {
            return type_string();
        }

        if (is_int($value)) {
            return type_integer();
        }

        if (is_bool($value)) {
            return type_boolean();
        }

        if (is_float($value)) {
            return type_float();
        }

        if (is_array($value)) {
            if ([] === $value) {
                // The bottom element type, not a distinct array shape: DuckDB says LIST(SQLNULL),
                // Spark says ArrayType(NullType). It unifies with any list, where array{} does not.
                return type_list(type_null());
            }

            $detector = new ArrayContentDetector(
                types(...array_map($this->detectType(...), array_keys($value)))->deduplicate(),
                types(...array_map($this->detectType(...), array_values($value)))->deduplicate(),
                array_is_list($value),
            );

            if ($detector->isList()) {
                $candidate = type_list($detector->valueType());

                return $candidate->isValid($value) ? $candidate : type_array();
            }

            if ($detector->isMap()) {
                $candidate = type_map($detector->firstKeyType(), $detector->valueType());

                // T resolves to array<array-key, mixed> here, so isValid()'s @assert-if-true narrows
                // nothing, but the runtime check still rejects values the key or value type refuses.
                // @mago-ignore analysis:redundant-type-comparison
                return $candidate->isValid($value) ? $candidate : type_array();
            }

            if ($detector->isStructure()) {
                $elements = [];

                // @mago-ignore analysis:mixed-assignment
                foreach ($value as $key => $item) {
                    $elements[type_string()->assert($key)] = $this->detectType($item);
                }

                $candidate = type_structure($elements);

                // T resolves to array<array-key, mixed> here, so isValid()'s @assert-if-true narrows
                // nothing, but the runtime check still rejects values an element type refuses.
                // @mago-ignore analysis:redundant-type-comparison
                return $candidate->isValid($value) ? $candidate : type_array();
            }

            return type_array();
        }

        if ($value instanceof UnitEnum) {
            return type_enum($value::class);
        }

        if (is_object($value)) {
            foreach (['Ramsey\Uuid\UuidInterface', 'Symfony\Component\Uid\Uuid'] as $uuidClass) {
                if (is_a($value, $uuidClass, true)) {
                    return type_uuid();
                }
            }

            if (type_uuid()->isValid($value)) {
                return type_uuid();
            }

            if (type_time()->isValid($value)) {
                return type_time();
            }

            if (type_time_zone()->isValid($value)) {
                return type_time_zone();
            }

            if (type_date()->isValid($value)) {
                return type_date();
            }

            if (type_datetime()->isValid($value)) {
                return type_datetime();
            }

            if (type_html()->isValid($value)) {
                return type_html();
            }

            if (type_html_element()->isValid($value)) {
                return type_html_element();
            }

            if (type_xml()->isValid($value)) {
                return type_xml();
            }

            if (type_xml_element()->isValid($value)) {
                return type_xml_element();
            }

            return type_instance_of($value::class);
        }

        throw new InvalidArgumentException('Unsupported type given: ' . gettype($value));
    }
}
