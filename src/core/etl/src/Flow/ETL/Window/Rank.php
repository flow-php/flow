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

        foreach ($partition->sortBy(...$windowSpec->order()) as $partitionRow) {
            if ($partitionRow->isEqual($row)) {
                return $rank;
            }

            $rank++;
        }

        return null;
    }
}
