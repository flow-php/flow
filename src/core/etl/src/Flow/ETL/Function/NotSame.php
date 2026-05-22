<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type\ValueComparator;

final class NotSame extends ScalarFunctionChain
{
    public function __construct(
        private readonly mixed $left,
        private readonly mixed $right,
    ) {}

    public function eval(Row $row, FlowContext $context): bool
    {
        $left = new Parameter($this->left);
        $right = new Parameter($this->right);

        (new ValueComparator())->assertComparableTypes(
            $left->asType($row, $context),
            $right->asType($row, $context),
            '!==',
        );

        return $left->eval($row, $context) !== $right->eval($row, $context);
    }
}
