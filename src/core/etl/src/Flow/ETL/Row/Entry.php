<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Row\Schema\Definition;
use Flow\Serializer\Serializable;

/**
 * @template TValue
 * @template TSerialized
 *
 * @extends Serializable<TSerialized>
 */
interface Entry extends Serializable
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

    public function ref() : EntryReference;

    public function rename(string $name) : self;

    public function toString() : string;

    /**
     * @return TValue
     */
    public function value();
}
