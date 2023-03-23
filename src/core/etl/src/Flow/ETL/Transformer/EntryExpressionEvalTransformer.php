<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Row\Reference\Expression\Literal;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

/**
 * @implements Transformer<array{entry_name: string, ref: EntryReference|Literal, entry_factory: EntryFactory}>
 */
final class EntryExpressionEvalTransformer implements Transformer
{
    public function __construct(
        private readonly string $entryName,
        private readonly EntryReference|Literal $ref,
        private readonly EntryFactory $entryFactory = new NativeEntryFactory()
    ) {
    }

    public function __serialize() : array
    {
        return [
            'entry_name' => $this->entryName,
            'ref' => $this->ref,
            'entry_factory' => $this->entryFactory,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->entryName = $data['entry_name'];
        $this->ref = $data['ref'];
        $this->entryFactory = $data['entry_factory'];
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(
            fn (Row $r) : Row => $r->set(
                $this->entryFactory->create($this->entryName, $this->ref instanceof Literal ? $this->ref->value() : $this->ref->eval($r))
            )
        );
    }
}
