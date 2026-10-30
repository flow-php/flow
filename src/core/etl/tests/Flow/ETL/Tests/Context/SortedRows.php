<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Kernels\Sort\RowOrder;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Values\Rows;

use function array_map;
use function array_values;

final class SortedRows
{
    /**
     * $rows in the order RowOrder gives them by $columns, for comparing results that carry no order.
     */
    public static function of(Rows $rows, Reference|SortColumn ...$columns): Rows
    {
        $order = new RowOrder(array_map(static fn(Reference|SortColumn $column): SortColumn => $column
            instanceof SortColumn
                ? $column
                : $column->asc(), array_values($columns)));

        return $rows->gather($order->permutation($order->keys($rows), $rows->count()));
    }
}
