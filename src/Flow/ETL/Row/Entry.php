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
     * @phpstan-ignore-next-line
     * @psalm-suppress MissingReturnType
     */
    public function value();

    public function map(callable $mapper) : self;

    public function isEqual(self $entry) : bool;
}
