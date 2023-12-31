<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class RemoveEntriesTransformer implements Transformer
{
    private readonly References $refs;

    public function __construct(string|Reference ...$names)
    {
        $this->refs = References::init(...$names);
    }

    public function __serialize() : array
    {
        return [
            'refs' => $this->refs,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->refs = $data['refs'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $transformer = fn (Row $row) : Row => $row->remove(...$this->refs);

        return $rows->map($transformer);
    }
}
