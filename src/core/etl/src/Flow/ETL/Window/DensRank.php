<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class DensRank implements WindowFunction
{
    public function apply(Row $row, Rows $partition, Window $window) : mixed
    {
        $rank = 1;

        $orderBy = $window->order();

        if (\count($orderBy) > 1) {
            throw new \RuntimeException('Dens Rank window function supports only one order by column');
        }

        if (\count($orderBy) === 0) {
            throw new \RuntimeException('Dens Rank window function requires to be ordered by one column');
        }

        $value = $row->valueOf($orderBy[0]->name());

        $countedValues = [];

        foreach ($partition->sortBy(...$window->order()) as $partitionRow) {

            $partitionValue = $partitionRow->valueOf($orderBy[0]->name());

            if ($value < $partitionValue) {
                if (!\in_array($partitionValue, $countedValues, true)) {
                    $rank++;
                    $countedValues[] = $partitionValue;
                }
            }
        }

        return $rank;
    }

    public function toString() : string
    {
        return 'dens_rank()';
    }
}
