<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row\{Reference, References};
use Flow\ETL\{FlowContext, Row, Rows, Transformer};

final readonly class DropEntriesTransformer implements Transformer
{
    private References $refs;

    public function __construct(string|Reference ...$names)
    {
        $this->refs = References::init(...$names);
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = fn (Row $row) : Row => $row->remove(...$this->refs);

        return $rows->map($transformer);
    }
}
