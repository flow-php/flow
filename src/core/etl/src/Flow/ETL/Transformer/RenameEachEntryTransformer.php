<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Transformer, Transformer\Rename\RenameEntryStrategy};

final readonly class RenameEachEntryTransformer implements Transformer
{
    public function __construct(
        private RenameEntryStrategy $strategy,
    ) {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(function (Row $row) use ($context) : Row {
            foreach ($row->entries()->all() as $entry) {
                $row = $this->strategy->rename($row, $entry, $context);
            }

            return $row;
        });
    }
}
