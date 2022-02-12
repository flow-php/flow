<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\Serializer\Serializable;

/**
 * @psalm-immutable
 */
interface Entry extends Serializable
{
    public function __toString() : string;

    public function __serialize() : array;

    public function __unserialize(array $data) : void;

    public function name() : string;

    public function rename(string $name) : self;

    public function is(string $name) : bool;

    /**
     * @return mixed
     */
    public function value();

    /**
     * @psalm-param pure-callable $mapper
     */
    public function map(callable $mapper) : self;

    public function isEqual(self $entry) : bool;

    public function toString() : string;
}
