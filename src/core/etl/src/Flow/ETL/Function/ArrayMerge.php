<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Row;

/**
 * Scalar function that takes two other functions, checks if both of them are arrays and merges them.
 */
final class ArrayMerge extends ScalarFunctionChain
{
    public function __construct(private readonly ScalarFunction|array $left, private readonly ScalarFunction|array $right)
    {
    }

    public function eval(Row $row) : mixed
    {
        $left = (new Parameter($this->left))->asArray($row);
        $right = (new Parameter($this->right))->asArray($row);

        if ($left === null || $right === null) {
            return null;
        }

        return \array_merge($left, $right);
    }
}
