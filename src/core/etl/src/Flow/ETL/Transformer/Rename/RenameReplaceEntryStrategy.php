<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\{FlowContext, Row, Row\Entry};

final readonly class RenameReplaceEntryStrategy implements RenameEntryStrategy
{
    /**
     * @param array<string>|string $search
     * @param array<string>|string $replace
     */
    public function __construct(
        private string|array $search,
        private string|array $replace,
    ) {
    }

    public function rename(Row $row, Entry $entry, FlowContext $context) : Row
    {
        return $row->rename($entry->name(), \str_replace($this->search, $this->replace, $entry->name()));
    }
}
