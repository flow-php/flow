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
     * @param Type<mixed> $type
     */
    public function isEqual(self $type) : bool;

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
