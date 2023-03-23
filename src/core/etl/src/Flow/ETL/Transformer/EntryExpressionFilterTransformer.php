<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{ref: EntryReference}>
 */
final class EntryExpressionFilterTransformer implements Transformer
{
    public function __construct(
        private readonly EntryReference $ref
    ) {
    }

    public function __serialize() : array
    {
        return [
            'ref' => $this->ref,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->ref = $data['ref'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->filter(fn (Row $r) : bool => (bool) $this->ref->eval($r));
    }
}
