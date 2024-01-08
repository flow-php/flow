<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\StyleConverter\StringStyles;
use Jawira\CaseConverter\Convert;

final class EntryNameStyleConverterTransformer implements Transformer
{
    public function __construct(private readonly StringStyles $style)
    {
        if (!\class_exists(Convert::class)) {
            throw new RuntimeException("Jawira\CaseConverter\Convert class not found, please add jawira/case-converter dependency to the project first.");
        }
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        $rowTransformer = function (Row $row) : Row {
            $valueMap = fn (Entry $entry) : Entry => $entry->rename(
                (string) \call_user_func([new Convert($entry->name()), 'to' . \ucfirst($this->style->value)])
            );

            return $row->map($valueMap);
        };

        return $rows->map($rowTransformer);
    }
}
