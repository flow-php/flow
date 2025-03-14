<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

/**
 * @template-covariant T of mixed
 */
interface Type
{
    /**
     * @return Type<T>
     */
    public static function fromArray(array $data) : self;

    /**
     * @param Type<mixed> $type
     */
    public function isComparableWith(self $type) : bool;

    /**
     * Checks if another type is compatible with this type. Nullability is validated from a schema evolution perspective.
     * This means that when current type is nullable and ther other type is not nullable, it is still compatible.
     * When given type is not nullable and current type is nullable, it is not compatible.
     *
     * @param Type<mixed> $type
     */
    public function isCompatible(self $type) : bool;

    /**
     * Checks if another type is equal to this type. Nullability is not considered in this comparison.
     *
     * @param Type<mixed> $type
     */
    public function isEqual(self $type) : bool;

    /**
     * Checks if another type is the same as this type, including nullability.
     *
     * @param Type<mixed> $type
     */
    public function isSame(self $type) : bool;

    public function isValid(mixed $value) : bool;

    /**
     * @return Type<T>
     */
    public function makeNullable(bool $nullable) : self;

    /**
     * @param Type<mixed> $type
     *
     * @return Type<mixed>
     */
    public function merge(self $type) : self;

    public function normalize() : array;

    public function nullable() : bool;

    public function toString() : string;
}
