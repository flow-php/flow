<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

/**
 * @psalm-immutable
 */
interface Entry
{
    public function name() : string;

    public function rename(string $name) : self;

    public function is(string $name) : bool;

    /**
     * @return mixed
     */
    public function value();

    public function map(callable $mapper) : self;

    public function isEqual(self $entry) : bool;
}
