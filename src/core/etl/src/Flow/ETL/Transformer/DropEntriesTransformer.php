<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Transformer};
use Flow\ETL\Row\{Reference, References};

final readonly class DropEntriesTransformer implements Transformer
{
    private References $refs;

    public function __construct(string|Reference ...$names)
    {
        $this->refs = References::init(...$names);
    }

    #[\Override]
    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = fn (Row $row) : Row => $row->remove(...$this->refs);

        return $rows->map($transformer);
    }
}
