<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row};

final class Literal extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $value,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        return $this->value;
    }
}
