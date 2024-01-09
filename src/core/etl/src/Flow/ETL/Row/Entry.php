<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Schema\Definition;

/**
 * @template TValue
 */
interface Entry extends \Stringable
{
    public function __toString() : string;

    public function definition() : Definition;

    public function is(string|Reference $name) : bool;

    public function isEqual(self $entry) : bool;

    /**
     * @psalm-param callable(TValue) : TValue $mapper
     */
    public function map(callable $mapper) : self;

    public function name() : string;

    public function ref() : Reference;

    public function rename(string $name) : self;

    public function toString() : string;

    public function type() : Type;

    /**
     * @return TValue
     */
    public function value();
}
