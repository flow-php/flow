<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;

/**
 * Entry expression that takes two other expressions, checks if both of them are arrays and merges them.
 */
final class ArrayMerge implements Expression
{
    public function __construct(private readonly Expression $left, private readonly Expression $right)
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
