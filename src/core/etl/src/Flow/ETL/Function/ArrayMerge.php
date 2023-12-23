<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

/**
 * Scalar function that takes two other functions, checks if both of them are arrays and merges them.
 */
final class ArrayMerge extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction $left, private readonly ScalarFunction $right)
    {
    }

    public function eval(Row $row) : mixed
    {
        $left = $this->left->eval($row);
        $right = $this->right->eval($row);

        if (!\is_array($left) || !\is_array($right)) {
            return null;
        }

        return \array_merge($left, $right);
    }
}
