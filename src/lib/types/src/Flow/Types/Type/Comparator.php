<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function Flow\Types\DSL\{type_equals, type_instance_of};
use Flow\Types\Type;
use Flow\Types\Type\Logical\{DateTimeType,
    DateType,
    JsonType,
    ListType,
    MapType,
    OptionalType,
    StructureType,
    TimeType};
use Flow\Types\Type\Native\{FloatType, IntegerType, NullType, StringType, UnionType};

final class Comparator
{
    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     */
    public function comparable(Type $left, Type $right) : bool
    {
        if ($left instanceof UnionType && $left->isOptionalType()) {
            return $this->comparable(type_instance_of(Type::class)->assert($left->types()->reduceOptionals()->first()), $right);
        }

        if ($right instanceof UnionType && $right->isOptionalType()) {
            return $this->comparable($left, type_instance_of(Type::class)->assert($right->types()->reduceOptionals()->first()));
        }

        if ($left instanceof UnionType || $right instanceof UnionType) {
            return false;
        }

        if ($left instanceof OptionalType) {
            return $this->comparable($left->base(), $right) || $right instanceof NullType;
        }

        if ($right instanceof OptionalType) {
            return $this->comparable($left, $right->base()) || $left instanceof NullType;
        }

        if ($left instanceof NullType || $right instanceof NullType) {
            return true;
        }

        if ($left instanceof IntegerType || $left instanceof FloatType) {
            return $right instanceof IntegerType || $right instanceof FloatType || $right instanceof StringType;
        }

        if ($right instanceof IntegerType || $right instanceof FloatType) {
            return $left instanceof StringType;
        }

        if ($left instanceof DateTimeType || $left instanceof DateType) {
            return $right instanceof DateTimeType || $right instanceof DateType;
        }

        if ($left instanceof TimeType && $right instanceof TimeType) {
            return true;
        }

        if (\in_array($left::class, [StringType::class, JsonType::class], true) && \in_array($right::class, [StringType::class, JsonType::class], true)) {
            return true;
        }

        return type_equals($left, $right);
    }

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     */
    public function equals(Type $left, Type $right) : bool
    {
        if ($left::class !== $right::class) {
            return false;
        }

        if ($left instanceof MapType && $right instanceof MapType) {
            return $this->equals($left->key(), $right->key()) && $this->equals($left->value(), $right->value());
        }

        if ($left instanceof ListType && $right instanceof ListType) {
            return $this->equals($left->element(), $right->element());
        }

        if ($left instanceof StructureType && $right instanceof StructureType) {
            if (\count($left->elements()) !== \count($right->elements())) {
                return false;
            }

            $rightElements = $right->elements();

            foreach ($left->elements() as $name => $field) {
                if (!\array_key_exists($name, $rightElements)) {
                    return false;
                }

                if (!$this->equals($field, $rightElements[$name])) {
                    return false;
                }
            }

            return true;
        }

        return $left->toString() === $right->toString();
    }

    /**
     * @template T
     *
     * @param Type<T> $type
     * @param class-string<Type<mixed>> $typeClass
     */
    public function is(Type $type, string $typeClass) : bool
    {
        if ($type instanceof $typeClass) {
            return true;
        }

        if ($type instanceof OptionalType) {
            return $this->is($type->base(), $typeClass);
        }

        if ($type instanceof UnionType) {
            foreach ($type->types()->all() as $nextType) {
                if ($nextType instanceof $typeClass) {
                    return true;
                }
            }

            return false;
        }

        return false;
    }

    /**
     * @template T
     *
     * @param Type<T> $type
     * @param class-string<Type<mixed>> $typeClass
     * @param class-string<Type<mixed>> ...$typeClasses
     */
    public function isAny(Type $type, string $typeClass, string ...$typeClasses) : bool
    {
        $classes = [$typeClass, ...$typeClasses];

        foreach ($classes as $class) {
            if ($this->is($type, $class)) {
                return true;
            }
        }

        return false;
    }
}
