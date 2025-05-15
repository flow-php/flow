<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\{FlowContext, Row, Rows, Transformer};
use Flow\ETL\Row\Entry;
use Flow\Types\Type\AutoCaster;

final readonly class AutoCastTransformer implements Transformer
{
    public function __construct(private AutoCaster $caster)
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(fn (Row $row) => $row->map(fn (Entry $entry) => $context->entryFactory()->create($entry->name(), $this->caster->cast($entry->value()))));
    }
}
