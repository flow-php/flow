<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\Serializer\Serializable;

/**
 * @template T
 *
 * @extends Serializable<T>
 */
interface Reference extends Serializable
{
    public function __toString() : string;

    public function as(string $alias) : self;

    public function hasAlias() : bool;

    public function name() : string;

    /**
     * @return array<EntryReference>|string
     */
    public function to() : string|array;

    public function is(self $ref) : bool;
}
