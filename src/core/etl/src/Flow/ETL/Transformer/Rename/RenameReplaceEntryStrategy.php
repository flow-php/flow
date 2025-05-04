<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\{FlowContext, Row, Row\Entry};

final readonly class RenameReplaceEntryStrategy implements RenameEntryStrategy
{
    public function __construct(
        private string $search,
        private string $replace,
    ) {
    }

    public function rename(Row $row, Entry $entry, FlowContext $context) : Row
    {
        return $row->rename($entry->name(), \str_replace($this->search, $this->replace, $entry->name()));
    }
}
