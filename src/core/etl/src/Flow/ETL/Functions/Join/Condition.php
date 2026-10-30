<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Join;

use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Rows;

interface Condition extends Describable
{
    /**
     * $left and $right hold the same number of rows; pair i is (left row i, right row i).
     *
     * @return list<bool>
     */
    public function compare(Rows $left, Rows $right): array;

    /**
     * @return array<Reference>
     */
    public function left(): array;

    /**
     * @return array<Reference>
     */
    public function right(): array;
}
