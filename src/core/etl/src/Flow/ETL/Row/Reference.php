<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

interface Reference
{
    public function __toString() : string;

    public function as(string $alias) : self;

    public function hasAlias() : bool;

    public function is(self $ref) : bool;

    public function name() : string;

    public function sort() : SortOrder;

    public function to() : string;
}
