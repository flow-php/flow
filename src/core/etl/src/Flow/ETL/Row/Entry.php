<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\Definition;

/**
 * @template TValue
 * @template TType
 */
interface Entry extends \Stringable
{
    public function __toString() : string;

    public function definition() : Definition;

    public function is(string|Reference $name) : bool;

    /**
     * @param Entry<TValue, TType> $entry
     */
    public function isEqual(self $entry) : bool;

    /**
     * @param callable(TValue) : TValue $mapper
     *
     * @return Entry<TValue, TType>
     */
    public function map(callable $mapper) : self;

    public function name() : string;

    public function ref() : Reference;

    /**
     * @return Entry<TValue, TType>
     */
    public function rename(string $name) : self;

    public function toString() : string;

    /**
     * @return Type<TType>
     */
    public function type() : Type;

    /**
     * @return TValue
     */
    public function value();

    /**
     * @param TValue $value
     *
     * @return Entry<TValue, TType>
     */
    public function withValue(mixed $value) : self;
}
