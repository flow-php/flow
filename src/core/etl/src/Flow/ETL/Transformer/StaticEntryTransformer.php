<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{entry: Entry}>
 */
final class StaticEntryTransformer implements Transformer
{
    public function __construct(private readonly Entry $entry)
    {
    }

    public function __serialize() : array
    {
        return [
            'entry' => $this->entry,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entry = $data['entry'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        /**
         * @psalm-var pure-callable(Row $row) : Row $transformer
         */
        $transformer = fn (Row $row) : Row => $row->set($this->entry);

        return $rows->map($transformer);
    }
}
