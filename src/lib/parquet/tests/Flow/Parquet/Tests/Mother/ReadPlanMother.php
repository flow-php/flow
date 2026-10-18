<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Tests\Double\InMemoryIndexes;

final class ReadPlanMother
{
    /**
     * The window over a file without page indexes.
     */
    public static function window(int $rows, int $offset = 0, ?int $limit = null): ReadPlan
    {
        return ReadPlan::window($rows, new InMemoryIndexes(), $offset, $limit);
    }
}
