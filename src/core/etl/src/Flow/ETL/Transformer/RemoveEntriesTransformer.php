<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{refs: array<EntryReference>}>
 */
final class RemoveEntriesTransformer implements Transformer
{
    /**
     * @var array<EntryReference>
     */
    private readonly array $refs;

    public function __construct(string|Reference ...$names)
    {
        $this->refs = EntryReference::initAll(...$names);
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
