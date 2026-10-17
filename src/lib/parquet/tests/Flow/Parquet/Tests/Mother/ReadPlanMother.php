<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\Reader\ReadPlan;

final class ReadPlanMother
{
    public static function window(int $rows, int $offset = 0, ?int $limit = null): ReadPlan
    {
        return ReadPlan::window($rows, $offset, $limit);
    }
}
