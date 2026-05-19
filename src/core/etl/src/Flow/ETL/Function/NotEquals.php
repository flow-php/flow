<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class NotEquals extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $left,
        private readonly mixed $right,
    ) {}

    public function eval(Row $row, FlowContext $context): bool
    {
        return !(new Equals($this->left, $this->right))->eval($row, $context);
    }
}
