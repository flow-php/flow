<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\{FlowContext, Row};

final class When extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $condition,
        private readonly mixed $then,
        private readonly mixed $else = null,
    ) {
    }

    public function eval(Row $row, FlowContext $context) : mixed
    {
        $condition = (new Parameter($this->condition))->asBoolean($row, $context);

        if ($condition) {
            return (new Parameter($this->then))->eval($row, $context);
        }

        if ($this->else) {
            return (new Parameter($this->else))->eval($row, $context);
        }

        return null;
    }
}
