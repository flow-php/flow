<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\{FlowContext, Row};
use Flow\ETL\Row\Entry;

interface RenameEntryStrategy
{
    /**
     * @param Entry<mixed, mixed> $entry
     */
    public function rename(Row $row, Entry $entry, FlowContext $context) : Row;
}
