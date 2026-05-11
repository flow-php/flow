<?php

declare(strict_types=1);

namespace Flow\ETL\Function\MatchCases;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\Parameter;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row;

final readonly class MatchCondition implements ScalarFunction
{
    public function __construct(
        private mixed $condition,
        private mixed $then,
    ) {}

    public function eval(Row $row, FlowContext $context): mixed
    {
        return (new Parameter($this->then))->eval($row, $context);
    }

    public function valid(Row $row, FlowContext $context): bool
    {
        return (new Parameter($this->condition))->asBoolean($row, $context);
    }
}
