<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

/**
 * @template TType
 */
interface Type
{
    public static function fromArray(array $data) : self;

    public function isComparableWith(self $type) : bool;

    public function isEqual(self $type) : bool;

    public function isValid(mixed $value) : bool;

    public function makeNullable(bool $nullable) : self;

    public function merge(self $type) : self;

    public function normalize() : array;

    public function nullable() : bool;

    public function toString() : string;
}
