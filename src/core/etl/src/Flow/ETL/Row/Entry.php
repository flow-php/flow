<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Schema\Definition;
use Flow\Types\Type;

/**
 * @template-covariant T
 */
interface Entry extends \Stringable
{
    public function __toString() : string;

    /**
     * @return Definition<T>
     */
    public function definition() : Definition;

    /**
     * @return Entry<T>
     */
    public function duplicate() : self;

    public function is(string|Reference $name) : bool;

    /**
     * @param Entry<mixed> $entry
     */
    public function isEqual(self $entry) : bool;

    /**
     * @return Entry<T>
     */
    public function map(callable $mapper) : self;

    public function name() : string;

    public function ref() : Reference;

    /**
     * @return Entry<T>
     */
    public function rename(string $name) : self;

    public function toString() : string;

    /**
     * @return Type<T>
     */
    public function type() : Type;

    /**
     * @return T
     */
    public function value();

    /**
     * @return Entry<T>
     */
    public function withValue(mixed $value) : self;
}
