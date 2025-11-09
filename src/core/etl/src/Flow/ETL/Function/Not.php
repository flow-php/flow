<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row};

final class Not extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $value)
    {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        return !(new Parameter($this->value))->eval($row, $context);
    }
}
