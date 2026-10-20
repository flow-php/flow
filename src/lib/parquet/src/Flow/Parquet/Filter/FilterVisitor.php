<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter;

/**
 * @template T
 */
interface FilterVisitor
{
    /**
     * @return T
     */
    public function comparison(Comparison $comparison): mixed;

    /**
     * @return T
     */
    public function all(All $all): mixed;

    /**
     * @return T
     */
    public function any(Any $any): mixed;

    /**
     * @return T
     */
    public function in(In $in): mixed;

    /**
     * @return T
     */
    public function isNull(IsNull $isNull): mixed;

    /**
     * @return T
     */
    public function not(Not $not): mixed;

    /**
     * @return T
     */
    public function startsWith(StartsWith $startsWith): mixed;
}
