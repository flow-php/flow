<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use function Symfony\Component\String\u;
use Flow\ETL\{FlowContext, Row, Row\Entry};

final readonly class RenameTransliterateEntryStrategy implements RenameEntryStrategy
{
    public function rename(Row $row, Entry $entry, FlowContext $context) : Row
    {
        return $row->rename($entry->name(), u($entry->name())->ascii()->toString());
    }
}
