<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use function Flow\ETL\DSL\{type_array,
    type_boolean,
    type_date,
    type_datetime,
    type_float,
    type_int,
    type_json,
    type_map,
    type_null,
    type_object,
    type_string,
    type_time,
    type_uuid,
    type_xml,
    type_xml_element};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\{ListType, StructureType};
use Flow\ETL\PHP\Type\Native\{ArrayType, EnumType};

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
                $keyTypes = new Types(...\array_map([$this, 'detectType'], \array_keys($value))),
                $valueTypes = new Types(...\array_map([$this, 'detectType'], \array_values($value))),
                \array_is_list($value)
            );

            if ($detector->isList()) {
                return new ListType(ListElement::fromType($detector->valueType()->makeNullable($valueTypes->has(type_null()))));
            }

            if ($detector->isMap()) {
                /**
                 * @phpstan-ignore-next-line
                 */
                return type_map($detector->firstKeyType(), $detector->valueType()->makeNullable($valueTypes->has(type_null())));
            }

            if ($detector->isStructure()) {
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

            return type_object($value::class);
        }

        throw InvalidArgumentException::because('Unsupported type given: ' . \gettype($value));
    }
}
