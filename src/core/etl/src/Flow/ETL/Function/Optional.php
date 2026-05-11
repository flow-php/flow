<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class Optional extends ScalarFunctionChain
{
    public function __construct(
        private readonly ScalarFunction $function,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        try {
            return (new Parameter($this->function))->eval($row, $context);
        } catch (\Exception) {
            return null;
        }
    }
}
