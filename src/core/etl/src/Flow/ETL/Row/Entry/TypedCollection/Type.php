<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Entry\TypedCollection;

/**
 * @psalm-immutable
 */
interface Type
{
    public function isEqual(self $type) : bool;

    /**
     * @param array<mixed> $collection
     */
    public function isValid(array $collection) : bool;

    public function toString() : string;
}
