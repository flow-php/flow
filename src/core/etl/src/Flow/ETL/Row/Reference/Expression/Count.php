<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression;
use Flow\ETL\Rows;
use Flow\ETL\Window;
use Flow\ETL\Window\WindowFunction;

final class Count implements Expression, WindowFunction
{
    public function __construct(private readonly Expression $ref)
    {
    }

    public function eval(Row $row) : mixed
    {
        /** @var mixed $val */
        $val = $this->ref->eval($row);

        if (\is_countable($val)) {
            return \count($val);
        }

        return null;
    }

    public function apply(Row $row, Rows $partition, Window $window): mixed
    {
        $count = 0;
        $value = $row->valueOf($this->ref);

        foreach ($partition->sortBy(...$window->order()) as $partitionRow) {
            $partitionValue = $partitionRow->valueOf($this->ref);

            if ($partitionValue === $value) {
                $count++;
            }
        }

        return $count;
    }

    public function toString(): string
    {
        return 'count()';
    }
}
