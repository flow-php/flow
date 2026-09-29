<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;

interface Comparison
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
