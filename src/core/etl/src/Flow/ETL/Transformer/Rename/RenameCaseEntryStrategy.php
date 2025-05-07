<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\{FlowContext, Function\StyleConverter\StringStyles, Row, Row\Entry};

final readonly class RenameCaseEntryStrategy implements RenameEntryStrategy
{
    public function __construct(
        private StringStyles $style,
    ) {
    }

    public function rename(Row $row, Entry $entry, FlowContext $context) : Row
    {
        return $row->rename($entry->name(), $this->style->convert($entry->name()));
    }
}
