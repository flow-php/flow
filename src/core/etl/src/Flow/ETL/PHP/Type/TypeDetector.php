<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Logical\List\ListElement;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\EnumType;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Native\ObjectType;
use Flow\ETL\PHP\Type\Native\ScalarType;

final class TypeDetector
{
    public function detectType(mixed $value) : Type
    {
        if (null === $value) {
            return new NullType();
        }

        if (\is_string($value)) {
            return ScalarType::string();
        }

        if (\is_int($value)) {
            return ScalarType::integer();
        }

        if (\is_bool($value)) {
            return ScalarType::boolean();
        }

        if (\is_float($value)) {
            return ScalarType::float();
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

                return new StructureType(...$elements);
            }

            return new ArrayType([] === \array_filter($value, fn ($value) : bool => null !== $value));
        }

        if ($value instanceof \UnitEnum) {
            return EnumType::of($value::class);
        }

        if (\is_object($value)) {
            return ObjectType::fromObject($value);
        }

        throw InvalidArgumentException::because('Unsupported type given: ' . \gettype($value));
    }
}
