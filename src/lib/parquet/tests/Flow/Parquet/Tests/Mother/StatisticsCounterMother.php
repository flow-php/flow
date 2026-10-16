<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Mother;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Writer\StatisticsCounter;
use Flow\Parquet\Writer\StatisticsTruncation;

final class StatisticsCounterMother
{
    /**
     * @param array<null|bool|float|int|string> $values
     */
    public static function counted(FlatColumn $column, array $values = [], int $truncateLength = 64): StatisticsCounter
    {
        $counter = new StatisticsCounter($column, new StatisticsTruncation($truncateLength));
        $counter->addBatch($values);

        return $counter;
    }
}
