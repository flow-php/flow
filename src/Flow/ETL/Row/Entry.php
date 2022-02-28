<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 */
interface Entry extends Serializable
{
    public function __serialize() : array;

    public function __toString() : string;

    public function __unserialize(array $data) : void;

    public function is(string $name) : bool;

    public function isEqual(self $entry) : bool;

    /**
     * @psalm-param pure-callable $mapper
     */
    public function map(callable $mapper) : self;

    public function name() : string;

    public function rename(string $name) : self;

    public function toString() : string;

    /**
     * @return mixed
     */
    public function value();
}
