<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\Doctrine\Bulk\BulkData;

interface BulkOperation
{
    public function execute(string $table, BulkData $bulkData) : void;
}
