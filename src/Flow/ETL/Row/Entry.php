<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Row\Schema\Definition;
use Flow\Serializer\Serializable;

/**
 * @template TValue
 * @template TSerialized
 * @extends Serializable<TSerialized>
 * @psalm-immutable
 */
interface Entry extends Serializable
{
    public function __toString() : string;

    public function definition() : Definition;

    public function is(string $name) : bool;

    /**
     * @param self $entry
     *
     * @return bool
     */
    public function isEqual(self $entry) : bool;

    /**
     * @psalm-param pure-callable(TValue) : TValue $mapper
     *
     * @return self
     */
    public function map(callable $mapper) : self;

    public function name() : string;

    /**
     * @param string $name
     *
     * @return self
     */
    public function rename(string $name) : self;

    public function toString() : string;

    /**
     * @return TValue
     */
    public function value();
}
