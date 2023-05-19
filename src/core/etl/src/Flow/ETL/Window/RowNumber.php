<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class RowNumber implements WindowFunction
{
    public function apply(Row $row, Rows $partition, Window $windowSpec) : mixed
    {
        $number = 1;

        foreach ($partition->sortBy(...$windowSpec->order()) as $partitionRow) {
            if ($partitionRow->isEqual($row)) {
                return $number;
            }

            $number++;
        }

        return null;
    }
}
