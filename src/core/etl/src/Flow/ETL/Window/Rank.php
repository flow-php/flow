<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class Rank implements WindowFunction
{
    public function apply(Row $row, Rows $partition, Window $windowSpec) : mixed
    {
        $rank = 1;

        $orderBy = $windowSpec->order();

        if (\count($orderBy) > 1) {
            throw new \RuntimeException('Rank window function supports only one order by column');
        }

        if (\count($orderBy) === 0) {
            throw new \RuntimeException('Rank window function requires to be ordered by one column');
        }

        $value = $row->valueOf($orderBy[0]->name());

        foreach ($partition->sortBy(...$windowSpec->order()) as $partitionRow) {

            $partitionValue = $partitionRow->valueOf($orderBy[0]->name());

            if ($value < $partitionValue) {
                $rank++;
            }
        }

        return $rank;
    }
}
