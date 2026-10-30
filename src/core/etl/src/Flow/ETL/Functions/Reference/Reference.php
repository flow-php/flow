<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Reference;

use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Values\NullsOrder;

interface Reference extends ScalarFunction
{
    public function __toString(): string;

    public function as(string $alias): self;

    public function asc(NullsOrder $nulls = NullsOrder::FIRST): SortColumn;

    public function base(): string;

    public function desc(NullsOrder $nulls = NullsOrder::LAST): SortColumn;

    public function hasAlias(): bool;

    public function is(self $ref): bool;

    public function name(): string;

    public function to(): string;
}
