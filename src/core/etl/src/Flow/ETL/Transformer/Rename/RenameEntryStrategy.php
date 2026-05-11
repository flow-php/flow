<?php

declare(strict_types=1);

namespace Flow\ETL\Transformer\Rename;

use Flow\ETL\Row;

interface RenameEntryStrategy
{
    public function rename(Row $row): Row;
}
