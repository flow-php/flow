<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

interface Reference
{
    public function as(string $alias) : self;

    public function hasAlias() : bool;

    public function name() : string;

    /**
     * @return array<string>|string
     */
    public function to() : string|array;
}
