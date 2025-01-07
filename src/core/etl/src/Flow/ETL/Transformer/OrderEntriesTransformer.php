<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use function Flow\ETL\DSL\row;
use Flow\ETL\Transformer\OrderEntries\Comparator;
use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final readonly class OrderEntriesTransformer implements Transformer
{
    public function __construct(private Comparator $comparator)
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(function (Row $row) : Row {
            $entries = $row->entries()->all();

            usort($entries, fn ($left, $right) => $this->comparator->compare($left, $right));

            return row(...$entries);
        });
    }
}
