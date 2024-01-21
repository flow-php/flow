<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\FlowContext;
use Flow\ETL\PHP\Type\AutoCaster;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class AutoCastTransformer implements Transformer
{
    public function __construct(private readonly AutoCaster $caster)
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(function (Row $row) use ($context) {
            return $row->map(function (Entry $entry) use ($context) {
                if (!$entry instanceof StringEntry) {
                    return $entry;
                }

                return $context->entryFactory()->create($entry->name(), $this->caster->cast($entry->value()));
            });
        });
    }
}
