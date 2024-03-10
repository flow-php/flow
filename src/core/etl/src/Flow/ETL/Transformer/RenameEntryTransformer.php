<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final class RenameEntryTransformer implements Transformer
{
    public function __construct(private readonly string $from, private readonly string $to)
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(fn (Row $row) : Row => $row->rename($this->from, $this->to));
    }
}
