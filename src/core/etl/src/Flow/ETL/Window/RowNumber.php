<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class RowNumber implements WindowFunction
{
    public function apply(Row $row, Rows $partition, Window $window) : mixed
    {
        $number = 1;

        foreach ($partition->sortBy(...$window->order()) as $partitionRow) {
            if ($partitionRow->isEqual($row)) {
                return $number;
            }

            $number++;
        }

        return null;
    }

    public function toString() : string
    {
        return 'row_number()';
    }
}
