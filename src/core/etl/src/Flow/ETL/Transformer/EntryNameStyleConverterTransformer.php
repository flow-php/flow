<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Row\Entry;
use Flow\ETL\{FlowContext, Function\StyleConverter\StringStyles, Row, Rows, Transformer};

final class EntryNameStyleConverterTransformer implements Transformer
{
    public function __construct(private readonly StringStyles $style)
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $rowTransformer = function (Row $row) : Row {
            $valueMap = fn (Entry $entry) : Entry => $entry->rename($this->style->convert($entry->name()));

            return $row->map($valueMap);
        };

        return $rows->map($rowTransformer);
    }
}
