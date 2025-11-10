<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Row};

/**
 * Scalar function that takes two other functions, checks if both of them are arrays and merges them.
 */
final class ArrayMerge extends ScalarFunctionChain
{
    /**
     * @param array<array-key, mixed>|ScalarFunction $left
     * @param array<array-key, mixed>|ScalarFunction $right
     */
    public function __construct(private readonly ScalarFunction|array $left, private readonly ScalarFunction|array $right)
    {
    }

    /**
     * @return null|array<mixed>
     */
    public function eval(Row $row, FlowContext $context) : mixed
    {
        $left = (new Parameter($this->left))->asArray($row, $context);
        $right = (new Parameter($this->right))->asArray($row, $context);

        if ($left === null || $right === null) {
            return $context->functions()->invalidResult(new InvalidArgumentException('ArrayMerge function requires two non-null arrays'));
        }

        return \array_merge($left, $right);
    }
}
