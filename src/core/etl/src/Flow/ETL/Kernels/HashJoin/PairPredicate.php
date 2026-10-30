<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\HashJoin;

use Flow\ETL\Values\Rows;

interface PairPredicate
{
    /**
     * @param list<int> $leftIndices
     * @param list<int> $rightIndices pairwise with $leftIndices
     *
     * @return list<bool> pair i is (left row $leftIndices[i], right row $rightIndices[i])
     */
    public function meet(Rows $left, array $leftIndices, Rows $right, array $rightIndices): array;
}
