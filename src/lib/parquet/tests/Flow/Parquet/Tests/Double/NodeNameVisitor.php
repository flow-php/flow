<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Double;

use Flow\Parquet\Filter\All;
use Flow\Parquet\Filter\Any;
use Flow\Parquet\Filter\Comparison;
use Flow\Parquet\Filter\FilterVisitor;
use Flow\Parquet\Filter\In;
use Flow\Parquet\Filter\IsNull;
use Flow\Parquet\Filter\Not;
use Flow\Parquet\Filter\StartsWith;

/**
 * Answers the name of the visitor method a node called.
 *
 * @implements FilterVisitor<string>
 */
final class NodeNameVisitor implements FilterVisitor
{
    public function comparison(Comparison $comparison): string
    {
        return 'comparison';
    }

    public function all(All $all): string
    {
        return 'all';
    }

    public function any(Any $any): string
    {
        return 'any';
    }

    public function in(In $in): string
    {
        return 'in';
    }

    public function isNull(IsNull $isNull): string
    {
        return 'isNull';
    }

    public function not(Not $not): string
    {
        return 'not';
    }

    public function startsWith(StartsWith $startsWith): string
    {
        return 'startsWith';
    }
}
