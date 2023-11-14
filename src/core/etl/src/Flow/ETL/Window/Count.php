<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class Count implements WindowFunction
{
    public function __construct(private readonly Row\EntryReference $ref)
    {
    }

    public function apply(Row $row, Rows $partition, Window $windowSpec) : mixed
    {
        $count = 0;
        $value = $row->valueOf($this->ref);

        foreach ($partition->sortBy(...$windowSpec->order()) as $partitionRow) {
            $partitionValue = $partitionRow->valueOf($this->ref);

            if ($partitionValue === $value) {
                $count++;
            }
        }

        return $count;
    }
}
