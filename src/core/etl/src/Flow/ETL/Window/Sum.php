<?php

declare(strict_types=1);

namespace Flow\ETL\Window;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class Sum implements WindowFunction
{
    public function __construct(private readonly Row\EntryReference $ref)
    {
    }

    public function apply(Row $row, Rows $partition, Window $windowSpec) : mixed
    {
        $sum = 0;

        foreach ($partition->sortBy(...$windowSpec->order()) as $partitionRow) {
            /** @var mixed $value */
            $value = $partitionRow->valueOf($this->ref);

            if (\is_numeric($value)) {
                $sum += $value;
            }
        }

        return $sum;
    }
}
